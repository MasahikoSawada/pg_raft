#ifndef PG_RAFT_H
#define PG_RAFT_H

#include "nodes/pg_list.h"
#include "comm.h"
#include "raft_internal.h"

#define PGR_DEBUG LOG

#define PG_RAFT_SCHEMA_NAME "pgraft"

/* GUC parameters */
extern char *pg_raft_database;

/* pgraft.node table */
#define TableName_node "node"
#define Nattrs_node 5
#define Anum_node_node_id	1
#define Anum_node_node_name 2
#define Anum_node_node_dns 	3
#define Anum_node_group_id	4
#define Anum_node_is_local	5

/* pgraft.node_group table */
#define TableName_node_group "node_group"
#define Nattrs_node_group 2
#define Anum_node_group_group_id	1
#define Anum_node_group_group_name	2

/* pgraft.node_state table */
#define TableName_node_state "node_state"
#define Nattrs_node_state 2
#define Anum_node_state_current_term	1
#define Anum_node_state_voted_for		2

/* pgraft._log_entry_raw table */
#define TableName_log_entry_raw "_log_entry_raw"
#define Nattrs_log_entry_raw 4
#define Anum_log_entry_raw_term		1
#define Anum_log_entry_index		2
#define Anum_log_entry_data			3
#define Anum_log_entry_committed	4

typedef struct PgRaftNode
{
	/* These fields are persistent */
	NodeId	id;
	char *name;
	char *dns;
	uint32	group_id;
	bool is_local;
} PgRaftNode;

typedef struct PgRaftGroup
{
	NodeId	id;
	char *name;
} PgRaftGroup;

typedef struct NodeLogIndexes
{
	NodeId	id;	/* hash key */
	LogIndex next_index;
	LogIndex match_index;
} NodeLogIndexes;

enum
{
	ELECTION_IN_PROGRESS = 0,
	ELECTION_SUCCESS,
	ELECTION_RETRY,
	ELECTION_FAILED
};

typedef struct ElectionState
{
	int		state;
	TimestampTz start_ts;
	Term	proposed_term;
	int		num_nodes;
	int		num_votes;
} ElectionState;

typedef struct ProposeMessageState
{
	int		num_nodes;
	int		num_votes;
	bool	failed;
} ProposeMessageState;

typedef struct RaftServer
{
	PgRaftNode *node;	/* entry of pgraft.node */
	Comm		*comm;
	List		*remote_nodes;

	RaftRole	role;

	long		hb_interval;

	/*
	 * These values have to be persistent to the table, pgraft.node_state
	 */
	struct
	{
		Term 	current_term;
		NodeId	voted_for;
	} persistent;

	LogIndex commit_index;
	Term	commit_term;
	LogIndex last_applied;
	Term	last_applied_term;

	/* Fields used only by leader */
	TimestampTz	last_hb_time;
	HTAB	*node_indexes;	/* A list of NodeLogIndexes */
	ProposeMessageState propose_state;

	/* Fields used only by follower */
	TimestampTz	last_received_time;
	TimestampTz voted_time;

	/* Fields used only by candidate */
	ElectionState election_state;
} RaftServer;
extern RaftServer *MyRaftServer;

PGDLLEXPORT void pgr_worker_main(Datum main_arg);

extern Oid pgr_get_table_oid(const char *tablename, bool missing_ok);
extern PgRaftNode *pgr_get_local_node(void);
extern List *pgr_get_all_remote_nodes(uint32 group_id);

/* node_state */
extern void pgr_initialize_node_state(void);
extern bool pgr_get_node_state(Term *term_p, NodeId *voted_for_p);
extern void pgr_update_node_state(Term term, NodeId voted_for);

/* log_entry */
extern void pgr_commit_log_entries(LogIndex upto);
extern void pgr_append_log_entry(Term term, LogIndex index, text *data);

#endif
