/*-------------------------------------------------------------------------
 *
 * comm.c
 *		An intermediate communication layer between the local raft worker
 *		and remote workers.
 *
 * This module provides a transparent communication layer between the local
 * raft worker and remote raft workers. The comm module consists of the queue
 * using dslist for incoming messages and comm_queue_write() SQL function to
 * outbound messages.
 *
 * When sending a message to the remote node, it calls comm_queue_write() SQL
 * function while setting the data as function arguments on the remote node.
 * The function enqueues the message to the remote node's incoming queue.
 * When reading a message, it pops a message from the incoming message queue
 * that can be fetched by comm_get_queue().
 *
 * One might think that the nodes can connect directly each other instead of
 * having the intermediate communication layer. However, this requires the node
 * to open additional port and authenticate the peer.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/array.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/builtins.h"

#include "pg_raft.h"
#include "dslist.h"
#include "comm.h"

#define COMM_QUEUE_SIZE	64

static HTAB *QueuePool;

static int lwlock_tranche_id;

/* Saved hook values in case of unload */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* SQL-callable function */
PG_FUNCTION_INFO_V1(comm_write_queue);
PG_FUNCTION_INFO_V1(request_vote_rpc);
PG_FUNCTION_INFO_V1(append_entries_rpc);
PG_FUNCTION_INFO_V1(propose_message);

static void CommShmemRequest(void);
static void CommShmemStartup(void);

/*
 * Initialize the communication module. This function must be called by
 * the postmaster process only once at startup time.
 */
void
CommInitialize(void)
{
	dslist_initialize();

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = CommShmemRequest;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = CommShmemStartup;
}

static Size
CommShmemSize(void)
{
	return hash_estimate_size(COMM_QUEUE_SIZE, sizeof(QueuePoolEntry));
}

static void
CommShmemRequest(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(CommShmemSize());
}
static void
CommShmemStartup(void)
{
	HASHCTL info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	lwlock_tranche_id = LWLockNewTrancheId();
	LWLockRegisterTranche(lwlock_tranche_id, "Comm");

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	info.keysize = sizeof(uint32);
	info.entrysize = sizeof(QueuePoolEntry);
	info.num_partitions = 16;
	QueuePool = ShmemInitHash("queue pool hash",
							   COMM_QUEUE_SIZE / 2,
							   COMM_QUEUE_SIZE,
							   &info,
							   HASH_ELEM | HASH_BLOBS | HASH_PARTITION);

	LWLockRelease(AddinShmemInitLock);
}

void
CommDetach(Comm *comm)
{
	if (!comm)
		return;

	if (comm->ent)
		comm->ent->proc = NULL;

	dslist_detach(comm->queue);
}

/*
 * Create a communication module identified by the given id.
 */
Comm *
CommCreate(uint32 id)
{
	Comm *comm;
	QueuePoolEntry *ent;
	dslist *queue;
	bool found;
	HASHCTL info;

	ent = hash_search(QueuePool, &id, HASH_ENTER, &found);

	/* Initialize the pool entry, if not yet */
	if (!found)
	{
		dsa_area *area;

		/* create DSA and dslist on it */
		area = dsa_create(lwlock_tranche_id);
		dsa_pin(area);
		dsa_pin_mapping(area);
		queue = dslist_create(area);

		ent->area_handle = dsa_get_handle(area);
		ent->queue_dp = dslist_handle(queue);
	}
	else
	{
		dsa_area *area;

		area = dsa_attach(ent->area_handle);
		dsa_pin_mapping(area);
		queue = dslist_attach(area, ent->queue_dp);
	}

	Assert(ent->proc == NULL);
	ent->proc = MyProc;

	/* create per-backend data for the communication module */
	comm = (Comm *) palloc(sizeof(Comm));
	comm->id = id;
	comm->queue = queue;
	comm->ent = ent;

	/* initialize the connection map */
	info.keysize = sizeof(uint32);
	info.entrysize = sizeof(comm_conn_ent);
	info.hcxt = TopMemoryContext;
	comm->conns = hash_create("peer connections", 64, &info,
						   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	elog(PGR_DEBUG, "initialized comm with id %u", id);

	return comm;
}

/*
 * Add a remote connection information to the com module.
 */
void
CommAddConn(Comm *comm, uint32 id, const char *conninfo)
{
	PGconn *conn;
	comm_conn_ent *conn_ent;
	bool found;

	conn_ent = hash_search(comm->conns, &id, HASH_ENTER, &found);

	if (found)
	{
		/* finish the connection since the node's conninfo might change */
		if (conn_ent->conninfo)
			pfree(conn_ent->conninfo);
		if (PQstatus(conn_ent->conn) == CONNECTION_OK)
			PQfinish(conn_ent->conn);
	}

	conn_ent->conninfo = pstrdup(conninfo);
	conn = PQconnectdb(conninfo);

	if (!conn || PQstatus(conn) != CONNECTION_OK)
	{
		ereport(LOG, (errmsg("could not connect to server with id %u", id)));
		conn_ent->conn = NULL;
	}
	else
	{
		elog(LOG, "add conn %s with id %u", conninfo, id);
		conn_ent->conn = conn;
	}
}

static comm_conn_ent *
CommGetConnectionEntry(Comm *comm, uint32 id)
{
	comm_conn_ent *conn_ent;
	bool found;

	conn_ent = hash_search(comm->conns, &id, HASH_ENTER, &found);
	if (!found)
		return NULL;

	if (!conn_ent->conn || PQstatus(conn_ent->conn) != CONNECTION_OK)
	{
		PGconn *conn = PQconnectdb(conn_ent->conninfo);

		if (!conn || PQstatus(conn) != CONNECTION_OK)
		{
			ereport(LOG,
					(errmsg("could not connect to server with id %u, skip send message",
							id)));
			return NULL;
		}

		conn_ent->conn = conn;
	}

	return conn_ent;
}

static void
prepare_rpc_query(RaftRPCRequest *rpc, List *entries, StringInfo query)
{
	initStringInfo(query);

	if (rpc->type == RAFT_RPC_TYPE_APPEND_ENTRIES)
	{
		RaftRPCAppendEntries *ae = (RaftRPCAppendEntries *) rpc;
		ListCell *lc;

		appendStringInfo(query,
						 "SELECT term, success FROM pgraft.append_entries_rpc(%u, %u, %lu, %u, %lu, %lu, %lu",
						 ae->hdr.to, ae->hdr.from,
						 ae->term, ae->leader_id, ae->prev_log_index,
						 ae->prev_log_term, ae->leader_commit);

		foreach (lc, entries)
		{
			text *entry = (text *) lfirst(lc);

			appendStringInfo(query, ", encode('%s'::bytea, 'hex')",
							 text_to_cstring(entry));
		}
		appendStringInfo(query, ")");
	}
	else
	{
		RaftRPCRequestVote *rv = (RaftRPCRequestVote *) rpc;

		appendStringInfo(query,
						 "SELECT term, vote_granted FROM pgraft.request_vote_rpc(%u, %u, %lu, %u, %lu, %lu)",
						 rv->hdr.to, rv->hdr.from,
						 rv->term, rv->candidate_id,
						 rv->last_log_index, rv->last_log_term);
	}
}

/*
 * Asynchronous RPC call support.
 *
 * foreach (lc, nodes)
 * {
 *     if (CommCallRPCBegin(comm, rpc, entries))
 *         pending = lappend(pending, n->id);
 * }
 * foreach (lc, pending)
 * {
 *     ret = CommCallRPCEnd(comm, id, &term, &success);
 *     if (!ret)
 *         continue;
 *
 *     (process the result)
 * }
 */
bool
CommCallRPCBegin(Comm *comm, RaftRPCRequest *rpc, List *entries)
{
	comm_conn_ent *conn_ent;
	StringInfoData query;
	int res;

	conn_ent = CommGetConnectionEntry(comm, rpc->to);
	if (conn_ent == NULL)
		return false;

	/* prepare the query */
	prepare_rpc_query(rpc, entries, &query);

	res = PQsendQuery(conn_ent->conn, query.data);

	if (res == 0)
	{
		elog(WARNING, "could not call RPC type %d: %s",
			 rpc->type, PQerrorMessage(conn_ent->conn));
		return false;
	}

	return true;
}

/*
 * Wait for the previously-sent RPC to complete. Return false if we could not
 * get the result due to a connection issue. Return true on success, but please
 * note that *success_p could be false even if returning true.
 *
 * XXX: callback while waiting, or just call process_rpc_if_any while waiting.
 */
bool
CommCallRPCEnd(Comm *comm, uint32 to, Term *term_p, bool *success_p,
			   void callback(void))
{
	PGresult   *volatile last_res = NULL;
	comm_conn_ent *conn_ent;

	conn_ent = CommGetConnectionEntry(comm, to);
	Assert(conn_ent != NULL);

	PG_TRY();
	{
		for (;;)
		{
			PGresult *res;

			callback();

			while (PQisBusy(conn_ent->conn))
			{
				int			wc;

				/* Sleep until there's something to do */
				wc = WaitLatchOrSocket(MyLatch,
									   WL_LATCH_SET | WL_SOCKET_READABLE |
									   WL_EXIT_ON_PM_DEATH,
									   PQsocket(conn_ent->conn),
									   -1L, PG_WAIT_EXTENSION);
				ResetLatch(MyLatch);

				CHECK_FOR_INTERRUPTS();

				/* Data available in socket? */
				if (wc & WL_SOCKET_READABLE)
				{
					if (!PQconsumeInput(conn_ent->conn))
					{
						elog(WARNING, "could not read from the socket");
						return false;
					}
				}
			}

			res = PQgetResult(conn_ent->conn);
			if (res == NULL)
				break;			/* query is complete */

			PQclear(last_res);
			last_res = res;
		}
	}
	PG_CATCH();
	{
		if (last_res)
			PQclear(last_res);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (PQresultStatus(last_res) != PGRES_TUPLES_OK)
	{
		//elog(WARNING, "could not get RPC call result");
		return false;
	}

	/* Get the result, extract both term and success/vote_granted */
	Assert(PQntuples(last_res) == 1);
	Assert(PQnfields(last_res) == 2);

	/* Get Term */
	if (sscanf(PQgetvalue(last_res, 0, 0), "%lu", term_p) != 1)
	{
		elog(WARNING, "returned an invalid data as Term");
		return false;
	}

	/* Get Succes */
	if (strcmp(PQgetvalue(last_res, 0, 1), "t") == 0)
		*success_p = true;
	else
		*success_p = false;

	PQclear(last_res);
	return true;
}

/* Call the given RPC on the remote worker */
bool
CommCallRPC(Comm *comm, RaftRPCRequest *rpc, List *entries, Term *term_p,
			bool *success_p)
{
	comm_conn_ent *conn_ent;
	StringInfoData query;
	PGresult *res;

	conn_ent = CommGetConnectionEntry(comm, rpc->to);
	if (conn_ent == NULL)
		return false;

	/* construct the query */
	prepare_rpc_query(rpc, entries, &query);

	res = PQexec(conn_ent->conn, query.data);

	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		elog(WARNING, "could not call RPC type %d: %s",
			 rpc->type,
			 PQerrorMessage(conn_ent->conn));
		return false;
	}

	Assert(PQntuples(res) == 1);
	Assert(PQnfields(res) == 2);

	/* Get Term */
	if (sscanf(PQgetvalue(res, 0, 0), "%lu", term_p) != 1)
	{
		elog(WARNING, "returned an invalid data as Term");
		return false;
	}

	/* Get Succes */
	if (strcmp(PQgetvalue(res, 0, 1), "t") == 0)
		*success_p = true;
	else
		*success_p = false;

	PQclear(res);

	return true;
}

void *
CommPopRPCRequest(Comm *comm)
{
	dslist_elem *elem;

	elem = dslist_pop_elem(comm->queue);

	if (elem == NULL)
		return NULL;

	return (void *) &(elem->data);
}

/*
 * Enqueue the given RPC request and wait for the worker to finish the
 * RPC.
 */
static bool
CommReceiverCallRPC(const RaftRPCRequest *req, Term *term_p, bool *success_p)
{
	QueuePoolEntry *ent;
	dsa_area *area;
	dslist	*queue;
	dslist_elem *req_elem;
	RaftRPCRequest *req_shared;
	dsa_pointer req_dp;
	bool found;

	ent = hash_search(QueuePool, &(req->to), HASH_FIND, &found);
	if (!found)
		elog(ERROR, "the queue for id %u does not exist", req->to);

	/* Get the queue */
	area = dsa_attach(ent->area_handle);
	queue = dslist_attach(area, ent->queue_dp);

	/* there is no worker */
	if (ent->proc == NULL)
		return false;

	/* enqueue and let the worker know */
	req_elem = dslist_append_elem(queue, (void *) req, req->len, &req_dp);
	SetLatch(&ent->proc->procLatch);

	/* wait until the requested RPC is finished */
	req_shared = (RaftRPCRequest *) &(req_elem->data);
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		ResetLatch(MyLatch);

		/* procedure call finished */
		if (req_shared->called)
			break;

		WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
				  100, PG_WAIT_EXTENSION);
	}
	Assert(req_elem->next == InvalidDsaPointer);

	/* store the results */
	*term_p = req_shared->result.term;
	*success_p = req_shared->result.success;

	dsa_free(queue->area, req_dp);
	dslist_detach(queue);
	dsa_detach(area);

	return true;
}

Datum
append_entries_rpc(PG_FUNCTION_ARGS)
{
	uint32 to = PG_GETARG_INT64(0);
	uint32 from = PG_GETARG_INT64(1);
	Term term = PG_GETARG_INT64(2);
	uint32 leader_id = PG_GETARG_INT64(3);
	LogIndex prev_log_index = PG_GETARG_INT64(4);
	Term prev_log_term = PG_GETARG_INT64(5);
	LogIndex leader_commit = PG_GETARG_INT64(6);
	ArrayType *entries_arr = PG_ARGISNULL(7) ? NULL : PG_GETARG_ARRAYTYPE_P(7);
	RaftRPCAppendEntries *rpc;
	Datum *entries_datum;
	int nentries = 0;
	int entries_size = 0;
	int total_size;
	bool ret;
	Term ret_term;
	bool ret_success;
	Datum values[2];
	bool nulls[2] = {false};
	TupleDesc	tupdesc;
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Process the entries if there are */
	if (entries_arr != NULL)
	{
		Assert(ARR_NDIM(entries_arr) == 1);
		deconstruct_array_builtin(entries_arr, TEXTOID, &entries_datum,
								  NULL, &nentries);

		/* compute size required to store all text data */
		for (int i = 0; i < nentries; i++)
		{
			text *elem = DatumGetTextP(entries_datum[i]);
			entries_size += VARSIZE(elem);
			elog(NOTICE, "varsize %d total_size %d",
				 VARSIZE(elem), entries_size);
		}
	}

	total_size = SizeOfRaftRPCAppendEntries(entries_size);
	rpc = palloc0(total_size);

	/* prepare the header */
	rpc->hdr.type = RAFT_RPC_TYPE_APPEND_ENTRIES;
	rpc->hdr.to = to;
	rpc->hdr.from = from;
	rpc->hdr.len = total_size;
	rpc->hdr.owner = MyProc;
	rpc->hdr.called = false;

	/* fill the arguments */
	rpc->term = term;
	rpc->leader_id = leader_id;
	rpc->prev_log_index = prev_log_index;
	rpc->prev_log_term = prev_log_term;
	rpc->leader_commit = leader_commit;
	rpc->nentries = nentries;

	/* fill the text entries */
	if (nentries > 0)
	{
		char *ptr = (char *) &(rpc->entries);

		for (int i = 0; i < nentries; i++)
		{
			text *elem = DatumGetTextP(entries_datum[i]);
			memcpy(ptr, elem, VARSIZE(elem));
			ptr += VARSIZE(elem);
		}

		Assert(total_size == (ptr - (char *) rpc));
	}

	/* call Append Entries RPC */
	ret = CommReceiverCallRPC((RaftRPCRequest *) rpc, &ret_term,
							  &ret_success);

	if (!ret)
		ereport(ERROR,
				(errmsg("could not enqueue RPC request as there is no raft worker")));

	/* prepare the result */
	values[0] = Int64GetDatum(ret_term);
	values[1] = BoolGetDatum(ret_success);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum
request_vote_rpc(PG_FUNCTION_ARGS)
{
	uint32 to = PG_GETARG_INT64(0);
	uint32 from = PG_GETARG_INT64(1);
	Term term = PG_GETARG_INT64(2);
	uint32 candidate_id = PG_GETARG_INT64(3);
	LogIndex last_log_index = PG_GETARG_INT64(4);
	Term last_log_term = PG_GETARG_INT64(5);
	RaftRPCRequestVote rpc;
	Term ret_term;
	bool ret_vote_granted;
	bool ret;
	Datum values[2];
	bool nulls[2] = {false};
	TupleDesc	tupdesc;
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* prepare header */
	rpc.hdr.type = RAFT_RPC_TYPE_REQUEST_VOTE;
	rpc.hdr.to = to;
	rpc.hdr.from = from;
	rpc.hdr.len = sizeof(RaftRPCRequestVote);
	rpc.hdr.owner = MyProc;
	rpc.hdr.called = false;

	/* fill arguments */
	rpc.term = term;
	rpc.candidate_id = candidate_id;
	rpc.last_log_index = last_log_index;
	rpc.last_log_term = last_log_term;

	ret = CommReceiverCallRPC((RaftRPCRequest *) &rpc, &ret_term,
							  &ret_vote_granted);

	if (!ret)
		ereport(ERROR,
				(errmsg("could not enqueue RPC request as there is no raft worker")));

	/* prepare the result */
	values[0] = Int64GetDatum(ret_term);
	values[1] = BoolGetDatum(ret_vote_granted);
	tuple = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum
propose_message(PG_FUNCTION_ARGS)
{
	text *message = PG_GETARG_TEXT_P(0);
	RaftRPCProposeMessage *rpc;
	PgRaftNode *node;
	int total_size;
	Term dummy_term;
	bool success;
	bool ret;

	node = pgr_get_local_node();

	if (node == NULL)
		ereport(ERROR, (errmsg("no raft worker is running")));

	total_size = offsetof(RaftRPCProposeMessage, message) + VARSIZE(message);
	rpc = (RaftRPCProposeMessage *) palloc0(total_size);

	/* prepare the header */
	rpc->hdr.type = RAFT_RPC_TYPE_PROPOSE_MESSAGE;
	rpc->hdr.to = node->id;
	rpc->hdr.from = 0;
	rpc->hdr.len = total_size;
	rpc->hdr.owner = MyProc;
	rpc->hdr.called = false;
	memcpy(&(rpc->message), message, VARSIZE(message));

	ret = CommReceiverCallRPC((RaftRPCRequest *) rpc, &dummy_term, &success);

	if (!ret)
		ereport(ERROR,
				(errmsg("could not propose a new message")));

	PG_RETURN_BOOL(true);
}
