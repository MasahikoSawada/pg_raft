#ifndef RAFT_INTERNAL_H
#define RAFT_INTERNAL_H

typedef int32 NodeId;
#define InvalidNodeId	(-1)

/* Term. Starts from 1. */
typedef int64	Term;
#define InvalidTerm	((Term) (0))

/* The first index is 1 */
typedef int64	LogIndex;
#define InvalidLogIndex ((LogIndex) (0))

typedef enum
{
	RAFT_ROLE_LEADER = 0,
	RAFT_ROLE_CANDIDATE,
	RAFT_ROLE_FOLLOWER
} RaftRole;

typedef enum
{
	RAFT_RPC_TYPE_APPEND_ENTRIES = 0,
	RAFT_RPC_TYPE_REQUEST_VOTE,

	RAFT_RPC_TYPE_PROPOSE_MESSAGE,
} RaftRPCType;

typedef struct RaftRPCRequest
{
	RaftRPCType type;
	int32 	to;
	int32 	from;
	size_t	len; /* total request size including the header */

	/* the process who requested */
	PGPROC	*owner;

	/* this RPC is executed? set only by the worker */
	bool	called;

	/*
	 * 'result' is commonly used to store the results of the
	 * RPC call. Both AppendEntries RPC and RequestVote RPC use
	 * a Term and a boolean as the result.
	 */
	struct
	{
		Term 	term;
		bool 	success;
	} result;
} RaftRPCRequest;

typedef struct RaftRPCReqeustVote
{
	RaftRPCRequest hdr;

	Term	term;
	NodeId	candidate_id;
	LogIndex	last_log_index;
	Term	last_log_term;
} RaftRPCRequestVote;

typedef struct RaftRPCAppendEntries
{
	RaftRPCRequest hdr;

	Term		term;
	NodeId		leader_id;
	LogIndex	prev_log_index;
	Term		prev_log_term;
	LogIndex	leader_commit;
	int32		nentries;

	/* list of text containing hex-encoded binary data */
	text		entries[FLEXIBLE_ARRAY_MEMBER];
} RaftRPCAppendEntries;
#define SizeOfRaftRPCAppendEntries(entries_size) \
	(offsetof(RaftRPCAppendEntries, entries) + (entries_size))

typedef struct RaftRPCProposeMessage
{
	RaftRPCRequest hdr;

	/* encoded in hex format */
	text	message[FLEXIBLE_ARRAY_MEMBER];
} RaftRPCProposeMessage;

#endif /* RAFT_INTERNAL_H */
