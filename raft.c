#include "postgres.h"

#include "raft.h"


void
send_heartbeat(uint32 to)
{
	RaftMsgAppendEntries msg;

	msg.hdr.type = RAFT_MSGTYPE_APPEND_ENTRIES;
	msg.hdr.to = to;
	msg.hdr.from = MyRaftServer->node->id;
	msg.hdr.sz = sizeof(RaftMsgAppendEntries);

	msg.term = MyRaftServer->current_term;
	msg.leader_id = MyRaftServer->node->id;
	msg.prev_log_index = 0;
	msg.prev_log_term = 0;
	msg.leader_commit = MyRaftServer->commit_index;
	msg.nentries = 0;

	CommSendMessage(MyRaftServer->comm, to, (void *) &msg, msg.hdr.sz);
}

void
send_append_entries_result(uint32 to, bool success)
{
	RaftMsgAppendEntriesResult msg;

	msg.hdr.type = RAFT_MSGTYPE_APPEND_ENTRIES_RESULT;
	msg.hdr.to = to;
	msg.hdr.from = MyRaftServer->node->id;
	msg.hdr.sz = sizeof(RaftMsgAppendEntriesResult);

	msg.term = MyRaftServer->current_term;
	msg.success = success;

	CommSendMessage(MyRaftServer->comm, to, (void *) &msg, msg.hdr.sz);
}

void
send_request_vote(uint32 to)
{
	RaftMsgRequestVote msg;

	msg.hdr.type = RAFT_MSGTYPE_REQUEST_VOTE;
	msg.hdr.to = to;
	msg.hdr.from = MyRaftServer->node->id;
	msg.hdr.sz = sizeof(RaftMsgRequestVote);

	msg.term = MyRaftServer->election_state.proposed_term;
	//msg.candidate_id = ;
	msg.last_log_index = MyRaftServer->current_index;
	msg.last_log_term = MyRaftServer->current_term;

	CommSendMessage(MyRaftServer->comm, to, (void *) &msg, msg.hdr.sz);
}


/* XXX: can we use MyRaftServer->current_term instead? */
void
send_request_vote_result(uint32 to, Term term, bool vote_granted)
{
	RaftMsgRequestVoteResult msg;

	msg.hdr.type = RAFT_MSGTYPE_REQUEST_VOTE_RESULT;
	msg.hdr.to = to;
	msg.hdr.from = MyRaftServer->node->id;
	msg.hdr.sz = sizeof(RaftMsgRequestVoteResult);

	msg.term = term;
	msg.vote_granted = vote_granted;

	CommSendMessage(MyRaftServer->comm, to, (void *) &msg, msg.hdr.sz);
}
