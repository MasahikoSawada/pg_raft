/*-------------------------------------------------------------------------
 *
 * worker.c
 *		A raft worker process.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "common/pg_prng.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/ps_status.h"
#include "utils/memutils.h"

#include "pg_raft.h"
#include "raft_internal.h"

#define PGR_HEARTBEAT_INTERVAL_BASE_MS (2 * 1000) /* 2 sec */
#define PGR_ELECTION_TIMEOUT_MS (5 * 1000) /* 5 sec */

RaftServer *MyRaftServer = NULL;

static Oid NodeTblOid = InvalidOid;
static bool NodeInfoIsValid = true;

static void pgr_process_rpc_if_any(void);
static void pgr_hb_callback(void);

static void
pgr_worker_shutdown(int code, Datum arg)
{
	if (!MyRaftServer)
		return;

	if (MyRaftServer->comm)
		CommDetach(MyRaftServer->comm);
}

static void
pgr_worker_invalidate_callback(Datum arg, Oid relid)
{
	if (!OidIsValid(relid) || relid == NodeTblOid)
	{
		ereport(DEBUG1,
				(errmsg("pg_raft node information is invalidated, relid %u",
						relid)));
		NodeInfoIsValid = false;
	}
}

static void
pgr_server_dump(const char *prefix)
{
	RaftServer *server = MyRaftServer; /* shorter line */
	StringInfoData buf;

	initStringInfo(&buf);

	appendStringInfo(&buf, "my node: id %u name \"%s\" dns \"%s\" group_id %u\n",
					 server->node->id, server->node->name,
					 server->node->dns, server->node->group_id);
	appendStringInfo(&buf, "role: %s\n",
					 server->role == RAFT_ROLE_LEADER ? "LEADER" :
					 server->role == RAFT_ROLE_CANDIDATE ? "CANDIDATE" :
					 "FOLLOWER");
	appendStringInfo(&buf, "persistent: current_term %lu voted_for %u\n",
					 server->persistent.current_term,
					 server->persistent.voted_for);
	appendStringInfo(&buf, "indexes: commit_index %lu term %lu last_applied %lu term %lu\n",
					 server->commit_index, server->commit_term,
					 server->last_applied, server->last_applied_term);
	appendStringInfo(&buf, "volatile: commit_index %lu last_applied %lu\n",
					 server->commit_index,
					 server->last_applied);
	appendStringInfo(&buf, "misc: hb_interval %ld\n",
					 server->hb_interval);
	appendStringInfo(&buf, "leader: last_hb_time %s\n",
					 timestamptz_to_str(server->last_hb_time));
	appendStringInfo(&buf, "follower: last_received_time %s\n",
					 timestamptz_to_str(server->last_received_time));
	appendStringInfo(&buf, "election_state: start_time %s, proposed_term %lu, num_nodes %d, num_votes %d\n",
					 timestamptz_to_str(server->election_state.start_ts),
					 server->election_state.proposed_term,
					 server->election_state.num_nodes,
					 server->election_state.num_votes);

	{
		ListCell *lc;
		int i = 0;

		appendStringInfo(&buf, "remote_nodes:\n");
		foreach (lc, server->remote_nodes)
		{
			PgRaftNode *node = (PgRaftNode *) lfirst(lc);
			NodeLogIndexes *node_index;

			node_index = hash_search(server->node_indexes, &node->id,
									 HASH_FIND, NULL);

			appendStringInfo(&buf, "    [%d] id %u name \"%s\" dns \"%s\" group_id %u next_index %lu match_index %lu\n",
							 i++, node->id, node->name, node->dns, node->group_id,
							 node_index->next_index, node_index->match_index);
		}
	}

	elog(LOG, "%s%s%s", prefix, prefix != NULL ? "\n" : "", buf.data);
}

/* Switch the server role to 'to' */
static void
pgr_switch_role(RaftRole to)
{
	MyRaftServer->role = to;

	if (to == RAFT_ROLE_LEADER)
	{
		ereport(LOG, (errmsg("node \"%s\" now became leader",
							 MyRaftServer->node->name)));
		set_ps_display_remove_suffix();
		set_ps_display_suffix(": leader");
		MyRaftServer->last_hb_time = 0;
		/* XXX initialize *next_index and *match_index */
	}
	else if (to == RAFT_ROLE_FOLLOWER)
	{
		set_ps_display_remove_suffix();
		set_ps_display_suffix(": follower");
		ereport(LOG, (errmsg("node \"%s\" now became follower",
							 MyRaftServer->node->name)));
	}
	else
	{
		set_ps_display_remove_suffix();
		set_ps_display_suffix(": candidate");
		ereport(LOG, (errmsg("node \"%s\" now became candidate",
							 MyRaftServer->node->name)));
	}
}

/* Update node information if necessary */
static void
maybe_update_nodes(void)
{
	MemoryContext old_ctx;
	ListCell *lc;

	if (NodeInfoIsValid)
		return;

	if (MyRaftServer->node)
	{
		pfree(MyRaftServer->node);
		MyRaftServer->node = NULL;
	}

	if (MyRaftServer->remote_nodes != NIL)
	{
		list_free_deep(MyRaftServer->remote_nodes);
		MyRaftServer->remote_nodes = NIL;
	}

	StartTransactionCommand();
	old_ctx = MemoryContextSwitchTo(TopMemoryContext);

	MyRaftServer->node = pgr_get_local_node();

	if (MyRaftServer->node == NULL)
	{
		MemoryContextSwitchTo(old_ctx);
		CommitTransactionCommand();
		NodeInfoIsValid = true;
		return;
	}

	MyRaftServer->remote_nodes =
		pgr_get_all_remote_nodes(MyRaftServer->node->group_id);

	if (message_level_is_interesting(PGR_DEBUG))
	{
		foreach (lc, MyRaftServer->remote_nodes)
		{
			PgRaftNode *n = (PgRaftNode *) lfirst(lc);

			elog(PGR_DEBUG, "loading remote node id %u name \"%s\" conninfo %s",
				 n->id, n->name, n->dns);
		}
	}

	/*
	 * Update the connections and re-connect if necessary. Also, we initialize
	 * node's indexes if we find a new node.
	 *
	 * XXX: need to GC node_indexes at some point.
	 */
	Assert(MyRaftServer->comm);
	foreach (lc, MyRaftServer->remote_nodes)
	{
		PgRaftNode *n = (PgRaftNode *) lfirst(lc);
		NodeLogIndexes *node_index;
		bool found;

		elog(LOG, "adding remote connection id %u conninfo %s", n->id, n->dns);
		CommAddConn(MyRaftServer->comm, n->id, n->dns);

		node_index = hash_search(MyRaftServer->node_indexes, &n->id,
								 HASH_ENTER, &found);

		if (!found)
		{
			node_index->next_index = MyRaftServer->last_applied;
			node_index->match_index = MyRaftServer->last_applied;
		}
	}

	MemoryContextSwitchTo(old_ctx);
	CommitTransactionCommand();

	pgr_server_dump("updated");

	NodeInfoIsValid = true;
}

/*
 * Startup MyRaftServer. Return true if we prepared MyRaftServer, otherwise return
 * false.
 */
static bool
pgr_server_startup(void)
{
	ListCell *lc;
	MemoryContext old_ctx;
	HASHCTL info;
	int ret;

	StartTransactionCommand();
	old_ctx = MemoryContextSwitchTo(TopMemoryContext);

	MyRaftServer = palloc0(sizeof(RaftServer));
	NodeTblOid = pgr_get_table_oid(TableName_node, true);
	MyRaftServer->node = pgr_get_local_node();

	if (MyRaftServer->node == NULL)
	{
		/* There is no raft node entry, exit */
		MemoryContextSwitchTo(old_ctx);
		return false;
	}

	/* initialize the communication module */
	MyRaftServer->comm = CommCreate(MyRaftServer->node->id);

	/* get the remote nodes */
	MyRaftServer->remote_nodes = pgr_get_all_remote_nodes(MyRaftServer->node->group_id);

	/* Get (or initialize) the persistent state */
	if (!pgr_get_node_state(&(MyRaftServer->persistent.current_term),
							&(MyRaftServer->persistent.voted_for)))
	{
		/*
		 * If this is the first time to startup, there is no persistent state.
		 * Initialize the term and voted_for.
		 */
		elog(PGR_DEBUG, "there is no node state");
		MyRaftServer->persistent.current_term = 1;
		MyRaftServer->persistent.voted_for = InvalidNodeId;
		pgr_initialize_node_state();
	}

	MyRaftServer->last_received_time = GetCurrentTimestamp();
	MyRaftServer->hb_interval = PGR_HEARTBEAT_INTERVAL_BASE_MS +
		pg_prng_uint64_range(&pg_global_prng_state, 0, 2000);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* get last_applied index and term */
	ret = SPI_execute("SELECT term, index FROM pgraft._log_entry_raw WHERE NOT committed ORDER BY index desc LIMIT 1",
					  true, 1);
	Assert(ret == SPI_OK_SELECT);
	if (SPI_processed != 1)
	{
		/* there is not entry yet */
		MyRaftServer->last_applied = InvalidLogIndex;
		MyRaftServer->last_applied_term = InvalidTerm;
	}
	else
	{
		bool isnull;

		MyRaftServer->last_applied_term =
			DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc,
										1, &isnull));
		Assert(!isnull);
		MyRaftServer->last_applied =
			DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[1],
										SPI_tuptable->tupdesc,
										1, &isnull));
		Assert(!isnull);
	}

	/* get commit_index */
	ret = SPI_execute("SELECT term, index FROM pgraft._log_entry_raw WHERE committed ORDER BY index desc LIMIT 1",
					  true, 1);
	Assert(ret == SPI_OK_SELECT);
	if (SPI_processed != 1)
	{
		MyRaftServer->commit_index = InvalidLogIndex;
		MyRaftServer->commit_term = InvalidTerm;
	}
	else
	{
		bool isnull;

		MyRaftServer->commit_term =
			DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc,
										1, &isnull));
		Assert(!isnull);
		MyRaftServer->commit_index =
			DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[1],
										SPI_tuptable->tupdesc,
										1, &isnull));
		Assert(!isnull);
	}

	PopActiveSnapshot();
	SPI_finish();

	/* create a hash table for remote nodes' indexes */
	info.keysize = sizeof(NodeId);
	info.entrysize = sizeof(NodeLogIndexes);
	info.hcxt = TopMemoryContext;
	MyRaftServer->node_indexes = hash_create("node indexes", 64, &info,
											 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* initialize the connections to remote nodes and their next/match indexes */
	foreach (lc, MyRaftServer->remote_nodes)
	{
		PgRaftNode *n = (PgRaftNode *) lfirst(lc);
		NodeLogIndexes *node_index;

		elog(LOG, "adding remote connection id %u conninfo %s", n->id, n->dns);
		CommAddConn(MyRaftServer->comm, n->id, n->dns);

		/* initialize node's next/match indexes */
		node_index = hash_search(MyRaftServer->node_indexes, &n->id,
								 HASH_ENTER, NULL);
		node_index->next_index = MyRaftServer->last_applied;
		node_index->match_index = MyRaftServer->last_applied;
	}

	MemoryContextSwitchTo(old_ctx);
	CommitTransactionCommand();

	return true;
}

static void
save_node_state(void)
{
	pgr_update_node_state(MyRaftServer->persistent.current_term,
						  MyRaftServer->persistent.voted_for);
}

/* Update the current term and persist the update */
static void
update_current_term(Term new_term)
{
	MyRaftServer->persistent.current_term = new_term;
	save_node_state();
}

/*
 * AppendEntries RPC call
 */
static bool
call_append_entries_rpc_begin(NodeId to, LogIndex prev_log_index,
							  Term prev_log_term, List *entries)
{
	RaftRPCAppendEntries rpc;

	rpc.hdr.type = RAFT_RPC_TYPE_APPEND_ENTRIES;
	rpc.hdr.to = to;
	rpc.hdr.from = MyRaftServer->node->id;

	rpc.term = MyRaftServer->persistent.current_term;
	rpc.leader_id = MyRaftServer->node->id;
	rpc.prev_log_index = prev_log_index;
	rpc.prev_log_term = prev_log_term;
	rpc.leader_commit = MyRaftServer->commit_index;

	return CommCallRPCBegin(MyRaftServer->comm, (RaftRPCRequest *) &rpc, entries);
}

/*
 * RequestVote RPC call
 */
static bool
call_request_vote_rpc_begin(NodeId to)
{
	RaftRPCRequestVote rpc;

	rpc.hdr.type = RAFT_RPC_TYPE_REQUEST_VOTE;
	rpc.hdr.to = to;
	rpc.hdr.from = MyRaftServer->node->id;

	rpc.term = MyRaftServer->persistent.current_term;
	rpc.candidate_id = MyRaftServer->node->id;
	rpc.last_log_index = MyRaftServer->last_applied;
	rpc.last_log_term = MyRaftServer->last_applied_term;

	return CommCallRPCBegin(MyRaftServer->comm, (RaftRPCRequest *) &rpc, NIL);
}

/*
 * This function is called while waiting for the response of RequestVote
 * RPC. We check the election timeout, and process RPCs if any.
 */
static void
pgr_election_callback(void)
{
	TimestampTz now = GetCurrentTimestamp();

	if (TimestampDifferenceExceeds(MyRaftServer->election_state.start_ts,
								   now, PGR_ELECTION_TIMEOUT_MS))
	{
		/*
		 * The election is timed out, will start a new election.
		 */
		MyRaftServer->election_state.state = ELECTION_RETRY;

		ereport(LOG,
				(errmsg("election for term %ld started at %s is timed out",
						MyRaftServer->election_state.proposed_term,
						timestamptz_to_str(MyRaftServer->election_state.start_ts))));
		return;
	}

	/*
	 * While process a RPC, it's possible that we switch to a follower.
	 */
	pgr_process_rpc_if_any();
}

static void
pgr_hb_callback(void)
{
}

/*
 * Check if we haven't received any message from the leader for a long
 * time. IOW, is it time to become a candidate and start a new election?
 */
static void
pgr_check_election_timeout(void)
{
	volatile ElectionState *election_state = &(MyRaftServer->election_state);
	TimestampTz now;
	List *pending = NIL;
	ListCell *lc;

	if (MyRaftServer->role != RAFT_ROLE_FOLLOWER)
		return;

	now = GetCurrentTimestamp();
	if (!TimestampDifferenceExceeds(MyRaftServer->last_received_time,
									now, PGR_ELECTION_TIMEOUT_MS))
		return;

retry:

	/*
	 * Since we haven't received any messages from the current leader
	 * for PGR_ELECTION_TIMEOUT_MS, we become a candidate and start
	 * the new election.
	 */
	MyRaftServer->persistent.current_term++;
	MyRaftServer->persistent.voted_for = MyRaftServer->node->id;
	save_node_state();

	election_state->start_ts = now;
	election_state->proposed_term = MyRaftServer->persistent.current_term;
	election_state->num_nodes = list_length(MyRaftServer->remote_nodes) + 1;
	election_state->num_votes = 1; /* voted by myself */
	election_state->state = ELECTION_IN_PROGRESS;

	pgr_switch_role(RAFT_ROLE_CANDIDATE);
	ereport(LOG, (errmsg("node \"%s\" starts a new election for term %ld with %d nodes",
						 MyRaftServer->node->name,
						 election_state->proposed_term,
						 election_state->num_nodes)));

	/*
	 * Call RequestVote RPC on all remote servers.
	 */
	foreach (lc, MyRaftServer->remote_nodes)
	{
		PgRaftNode *n = (PgRaftNode *) lfirst(lc);

		if (call_request_vote_rpc_begin(n->id))
			pending = lappend_int(pending, n->id);
	}

	foreach (lc, pending)
	{
		NodeId id = lfirst_int(lc);
		bool ret;
		Term term;
		bool vote_granted;

		/*
		 * Poll and receive the response. Note that pgr_election_callback could
		 * set the state to ELECTION_RETRY.
		 */
		ret = CommCallRPCEnd(MyRaftServer->comm, id, &term, &vote_granted,
							 pgr_election_callback);
		if (!ret)
			continue;

		/*
		 * If the election is done, consume remaining responses.
		 */
		if (election_state->state != ELECTION_IN_PROGRESS)
			continue;

		ereport(LOG,
				(errmsg("get RequestVote RPC result from node %d: term %ld, vote_granted %d",
						id, term, vote_granted)));

		if (!vote_granted)
		{
			/*
			 * Got the newer term, which means this election failed and
			 * there is another leader of the term. Update our term and
			 * become follower. Setting failed = true ignores all further
			 * responses.
			 */
			if (term > MyRaftServer->persistent.current_term)
			{
				election_state->state = ELECTION_FAILED;
				update_current_term(term);
				pgr_switch_role(RAFT_ROLE_FOLLOWER);
			}

			continue;
		}

		/*
		 * XXX: is it possible that voted_granted is true but the returned
		 * term is greater than the current term?
		 */
		election_state->num_votes++;
		Assert(term ==  MyRaftServer->persistent.current_term);

		if (election_state->num_votes > (election_state->num_nodes / 2))
		{
			/*
			 * We got the votes from the majority, so become the leader
			 * of this term.
			 */
			election_state->state = ELECTION_SUCCESS;
			pgr_switch_role(RAFT_ROLE_LEADER);

			/* don't return since we need to consume remaining responses */
			continue;
		}
	}

	if (election_state->state == ELECTION_RETRY)
	{
		ereport(LOG,
				(errmsg("election for term %ld is timed out, retry with a new term",
						election_state->proposed_term)));
		goto retry;
	}
}


/*
 * Broadcast heartbeat messages to all remote nodes at an interval of
 * MyRaftServer->hb_interval.
 */
static void
pgr_maybe_send_hb(void)
{
	List *pending = NIL;
	ListCell *lc;

	/* heartbeat is sent only by the leader */
	if (MyRaftServer->role != RAFT_ROLE_LEADER)
		return;

	if (!TimestampDifferenceExceeds(MyRaftServer->last_hb_time,
									GetCurrentTimestamp(),
									MyRaftServer->hb_interval))
		return;

	/* Call AppendEntries RPC with empty entry on remote nodes */
	foreach (lc, MyRaftServer->remote_nodes)
	{
		PgRaftNode *n = (PgRaftNode *) lfirst(lc);

		if (call_append_entries_rpc_begin(n->id, InvalidLogIndex,
										  InvalidTerm, NIL))
			pending = lappend_int(pending, n->id);
	}

	foreach (lc, pending)
	{
		NodeId id = lfirst_int(lc);
		Term term;
		bool success;
		bool ret;

		ret = CommCallRPCEnd(MyRaftServer->comm, id, &term,
							 &success, pgr_hb_callback);

		if (!ret)
			continue;

		/*
		 * If the response term is newer, we update our term
		 * and become a follower.
		 */
		if (term > MyRaftServer->persistent.current_term)
		{
			update_current_term(term);
			pgr_switch_role(RAFT_ROLE_FOLLOWER);
		}
	}

	MyRaftServer->last_hb_time = GetCurrentTimestamp();
}

static void
pgr_append_entries_callback(void)
{

}

static void
pgr_leader_process_rpc(RaftRPCRequest *rpc)
{
	Assert(MyRaftServer->role == RAFT_ROLE_LEADER);

	switch (rpc->type)
	{
		case RAFT_RPC_TYPE_APPEND_ENTRIES:
			{
				RaftRPCAppendEntries *ae = (RaftRPCAppendEntries *) rpc;

				/* The request must have been sent by the leader for an older term */
				Assert(ae->term <= MyRaftServer->persistent.current_term);
				rpc->result.term = MyRaftServer->persistent.current_term;
				rpc->result.success = false;

				break;
			}
		case RAFT_RPC_TYPE_REQUEST_VOTE:
			{
				RaftRPCRequestVote *rv = (RaftRPCRequestVote *) rpc;

				/* The request must have been sent by the leader for an older term */
				Assert(rv->term <= MyRaftServer->persistent.current_term);
				rpc->result.term = MyRaftServer->persistent.current_term;
				rpc->result.success = false;

				break;
			}
		case RAFT_RPC_TYPE_PROPOSE_MESSAGE:
			{
#ifdef UNUSED
				RaftRPCProposeMessage *pm = (RaftRPCProposeMessage *) rpc;
				volatile ProposeMessageState *propose_state = &(MyRaftServer->propose_state);
				List *pending = NIL;
				ListCell *lc;

				/*
				 * XXX: what if we already know there is no enough nodes?
				 * We can either anyway wait for nodes in a hope that
				 * majority nodes will be available soon, or reject the
				 * message request.
				 */

				/* apply in the local */
				pgr_append_log_entry(MyRaftServer->persistent.current_term,
									 MyRaftServer->last_applied,
									 pm->message);

				/*
				 * Increment the last_applied, the leader never overwrites the
				 * log entry.
				 */
				MyRaftServer->last_applied++;

				/* initialize state propose state */
				propose_state->num_nodes = list_length(MyRaftServer->remote_nodes);
				propose_state->num_votes = 1;
				propose_state->failed = false;

				/*
				 * Call AppendEntriesRPC in parallel. To get the consensus
				 * we need to call it at least on the majority of the servers.
				 */
				for (;;)
				{
					foreach (lc, MyRaftServer->remote_nodes)
					{
						PgRaftNode *n = (PgRaftNode *) lfirst(lc);

						if (list_member_int(pending, n->id))
							continue;

						/*
						 * XXX: should send log entries from next_index.
						 */
						if (call_append_entries_rpc_begin(n->id,
														  list_make1(&(pm->message))))
							pending = lappend_int(pending, n->id);
					}

					/* broard-casted to at least the majority of the servers? */
					if (propose_state->num_nodes / 2 < list_length(pending))
						break;

					elog(PGR_DEBUG, "call AE RPC on %d nodes but need at least on %d nodes, retry",
						 list_length(pending),
						 propose_state->num_nodes / 2);
				}

				elog(PGR_DEBUG, "called AE RPC on %d nodes", list_length(pending));

				/*
				 * Wait for all responses.
				 *
				 * XXX: There is another design choice for the consistency;
				 * We can return the result as soon as we get the results
				 * from the majority.
				 */
				foreach (lc, pending)
				{
					NodeId id = lfirst_int(lc);
					bool ret;
					Term term;
					bool success;

					ret = CommCallRPCEnd(MyRaftServer->comm, id, &term,
										 &success, pgr_append_entries_callback);

					if (!ret)
					{
						elog(PGR_DEBUG, "could not get result of AE RPC from %u", id);
						continue;
					}

					if (propose_state->failed)
						continue;

					if (!success)
					{
						NodeLogIndexes *node_index;

						/*
						 * If the response term is newer, we update our term
						 * and become a follower.
						 */
						if (term > MyRaftServer->persistent.current_term)
						{
							elog(PGR_DEBUG, "get term %lu greater than our term %lu, become a follower",
								 term, MyRaftServer->persistent.current_term);
							update_current_term(term);
							pgr_switch_role(RAFT_ROLE_FOLLOWER);
							propose_state->failed = true;
							continue;
						}

						/* adjust next_index of this server */
						node_index = hash_search(MyRaftServer->node_indexes, &id,
												 HASH_FIND, NULL);
						Assert(node_index);
						if (node_index->next_index > 0)
							node_index->next_index--;
						if (node_index->match_index > 0)
							node_index->match_index--;

						elog(PGR_DEBUG, "got NOT success from %u, decrement next_index %lu -> %lu",
							 id, node_index->next_index + 1, node_index->next_index);
						continue;
					}

					/* success! update node indexes */
					node_index = hash_search(MyRaftServer->node_indexes, &id,
											 HASH_FIND, NULL);
					Assert(node_index);
					node_index->next_index++;
					node_index->match_index++;

					/* got a vote */
					propose_state->num_votes++;
				}

				if (propose_state->num_votes > propose_state->num_nodes / 2)
				{
					/*
					 * Got OKs from the majority of the nodes. Commit this
					 * log.
					 */
					MyRaftServer->commit_index = MyRaftServer->last_applied - 1;
					pgr_commit_log_entries(MyRaftServer->commit_index);
					elog(PGR_DEBUG, "get success from the majority, commit log index %lu",
						 MyRaftServer->commit_index);

					/* not use the term to return */
					rpc->result.success = true;
				}
				else
					elog(PGR_DEBUG, "could not success from the majority");

				break;
#endif
			}
		default:
			elog(WARNING, "unknown RPC request type %d", rpc->type);
	}
}

static void
pgr_follower_process_rpc(RaftRPCRequest *rpc)
{
	Assert(MyRaftServer->role == RAFT_ROLE_FOLLOWER);

	switch (rpc->type)
	{
		case RAFT_RPC_TYPE_APPEND_ENTRIES:
			{
				RaftRPCAppendEntries *ae = (RaftRPCAppendEntries *) rpc;

				MyRaftServer->last_received_time = GetCurrentTimestamp();

				/* Update our term to the received term */
				if (ae->term > MyRaftServer->persistent.current_term)
				{
					update_current_term(ae->term);
					rpc->result.success = false;
					break;
				}

				if (ae->prev_log_index == MyRaftServer->last_applied &&
					ae->prev_log_term == MyRaftServer->last_applied_term)
				{
					/* XXX: apply all received log entries */
					rpc->result.success = true;
				}
				else
				{
					rpc->result.success = false;
				}

				rpc->result.term = MyRaftServer->persistent.current_term;
				break;
			}
		case RAFT_RPC_TYPE_REQUEST_VOTE:
			{
				RaftRPCRequestVote *rv = (RaftRPCRequestVote *) rpc;

				rpc->result.term = MyRaftServer->persistent.current_term;

				/* Don't vote if the candidate's term is older than mine */
				if (rv->term < MyRaftServer->persistent.current_term)
				{
					ereport(LOG,
							(errmsg("reject the vote request for an older term %ld",
									rv->term)));

					rpc->result.success = false; /* voteGranted = false */
				}

				/*
				 * Vote if we've not voted for none or we've already voted for this
				 * candidate, and the candidate has up-to-date logs.
				 */
				else if ((MyRaftServer->persistent.voted_for == InvalidNodeId ||
						  MyRaftServer->persistent.voted_for == rv->candidate_id) &&
						 rv->last_log_index >= MyRaftServer->last_applied &&
						 rv->last_log_term >= MyRaftServer->last_applied_term)
				{
					ereport(LOG,
							(errmsg("voted for %d for leader of term %ld",
									rv->candidate_id,
									rv->term)));

					rpc->result.success = true;
					MyRaftServer->persistent.voted_for = rv->candidate_id;
					save_node_state();

					MyRaftServer->last_received_time = GetCurrentTimestamp();
				}

				/* Otherwise not vote */
				else
					rpc->result.success = false;

				break;
			}
		default:
			elog(WARNING, "unknown RPC request type %d", rpc->type);
	}
}

static void
pgr_candidate_process_rpc(RaftRPCRequest *rpc)
{
	switch (rpc->type)
	{
		case RAFT_RPC_TYPE_APPEND_ENTRIES:
			{
                rpc->result.term = MyRaftServer->persistent.current_term;
                rpc->result.success = false;
				break;
			}
		case RAFT_RPC_TYPE_REQUEST_VOTE:
			{
				rpc->result.term = MyRaftServer->persistent.current_term;
				rpc->result.success = false;
				break;
			}
		default:
			elog(WARNING, "unknown RPC request type %d", rpc->type);
	}
}

static void
pgr_rpc_dump(RaftRPCRequest *rpc)
{
	StringInfoData buf;
	char *role = MyRaftServer->role == RAFT_ROLE_LEADER ? "LED" :
		MyRaftServer->role == RAFT_ROLE_FOLLOWER ? "FOL" : "CAN";

	if (PGR_DEBUG != LOG)
		return;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "[%s] HDR[to %u from %u len %zu called %d owner-pid %u]: ",
					 role, rpc->to, rpc->from, rpc->len, rpc->called, rpc->owner->pid);

	switch (rpc->type)
	{
		case RAFT_RPC_TYPE_APPEND_ENTRIES:
			{
				RaftRPCAppendEntries *ae = (RaftRPCAppendEntries *) rpc;

				appendStringInfo(&buf,
								 "AE term %lu leader_id %d prev_log_index %lu prev_log_term %lu leader_commit %lu nentries %d",
								 ae->term, ae->leader_id,
								 ae->prev_log_index, ae->prev_log_term,
								 ae->leader_commit, ae->nentries);
				break;
			}
		case RAFT_RPC_TYPE_REQUEST_VOTE:
			{
				RaftRPCRequestVote *rv = (RaftRPCRequestVote *) rpc;

				appendStringInfo(&buf,
								 "RV term %lu candidate_id %d last_log_index %lu last_log_term %lu",
								 rv->term, rv->candidate_id,
								 rv->last_log_index, rv->last_log_term);
				break;
			}
		case RAFT_RPC_TYPE_PROPOSE_MESSAGE:
			{
				appendStringInfo(&buf, "PM");
				break;
			}
	}

	elog(PGR_DEBUG, "%s", buf.data);
}

static void
pgr_process_rpc_if_any(void)
{
	RaftRPCRequest *req;
	Term rpc_term = InvalidTerm;

	pg_usleep(300 * 1000);

	req = (RaftRPCRequest *) CommPopRPCRequest(MyRaftServer->comm);

	if (req == NULL)
		return;

	if (req->to != MyRaftServer->node->id)
		elog(ERROR, "received a RPC request for node %u, but my id is %u",
			 req->to, MyRaftServer->node->id);

	pgr_rpc_dump(req);

	/*
	 * Regardless of our current role, become a follower if the RPC request
	 * has a newer term than ours.
	 *
	 * XXX: req->type might not have the term (e.g., proposing message).
	 */
	if (req->type == RAFT_RPC_TYPE_APPEND_ENTRIES)
		rpc_term = ((RaftRPCAppendEntries *) req)->term;
	else if (req->type == RAFT_RPC_TYPE_REQUEST_VOTE)
		rpc_term = ((RaftRPCRequestVote *) req)->term;

	if (rpc_term != InvalidTerm && rpc_term > MyRaftServer->persistent.current_term)
	{
		ereport(LOG,
				(errmsg("get RPC request with a newer term %ld than current term %ld",
						rpc_term, MyRaftServer->persistent.current_term)));

		MyRaftServer->persistent.current_term = rpc_term;
		MyRaftServer->persistent.voted_for = InvalidNodeId;
		save_node_state();
		pgr_switch_role(RAFT_ROLE_FOLLOWER);
	}

	switch (MyRaftServer->role)
	{
		case RAFT_ROLE_LEADER:
			pgr_leader_process_rpc(req);
			break;
		case RAFT_ROLE_FOLLOWER:
			pgr_follower_process_rpc(req);
			break;
		case RAFT_ROLE_CANDIDATE:
			pgr_candidate_process_rpc(req);
			break;
		default:
			elog(ERROR, "unexpected raft role type %d", MyRaftServer->role);
	}

	req->called = true;
	SetLatch(&(req->owner->procLatch));
}

static long
pgr_compute_sleep_time(void)
{
	TimestampTz now = GetCurrentTimestamp();
	TimestampTz wakeup_time;

	wakeup_time = TimestampTzPlusMilliseconds(now, MyRaftServer->hb_interval);

	wakeup_time = Min(wakeup_time,
					  TimestampTzPlusMilliseconds(now, PGR_ELECTION_TIMEOUT_MS));

	return TimestampDifferenceMilliseconds(now, wakeup_time);
}

void
pgr_worker_main(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection(pg_raft_database, NULL, 0);

	before_shmem_exit(pgr_worker_shutdown, 0);
	CacheRegisterRelcacheCallback(pgr_worker_invalidate_callback, (Datum) 0);

	/* Initialize my node */
	if (!pgr_server_startup())
	{
		elog(LOG, "could not find local pg_raft node, exit");
		proc_exit(0);
	}

	elog(LOG, "pg_raft worker for database \"%s\" with node name \"%s\" has started",
		 pg_raft_database, MyRaftServer->node->name);

	{
		char activitymsg[50] = {0};
		snprintf(activitymsg, sizeof(activitymsg), "%s(%u)",
				 MyRaftServer->node->name,
				 MyRaftServer->node->id);
		set_ps_display(activitymsg);
	}

	/* Always start as an follower */
	MyRaftServer->role = RAFT_ROLE_FOLLOWER;
	set_ps_display_suffix(": follower");

	pgr_server_dump("startup");

	/* Main loop */
	for (;;)
	{
		long sleep_time;

		CHECK_FOR_INTERRUPTS();

		AcceptInvalidationMessages();
		maybe_update_nodes();

		sleep_time = pgr_compute_sleep_time();

		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  sleep_time,
				  PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		/* Process RPC requests */
		pgr_process_rpc_if_any();

		/* Check the election timeout and could become a candidate */
		pgr_check_election_timeout();

		pgr_maybe_send_hb();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	proc_exit(0);
}

