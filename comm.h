#ifndef COMM_H
#define COMM_H

#include "storage/proc.h"

#include "libpq-fe.h"
#include "dslist.h"
#include "raft_internal.h"

/* Struct of a hash map element for queue_pool */
typedef struct QueuePoolEntry
{
	/* hash key */
	uint32	id;

	PGPROC	*proc;

	/* DSA handle where the queue allocated */
	dsa_handle	area_handle;

	/* DSA pointer for the queue */
	dsa_pointer	queue_dp;
} QueuePoolEntry;

typedef struct comm_conn_ent
{
	uint32	peer_id;
	char *conninfo;
	PGconn	*conn;
} comm_conn_ent;

typedef struct Comm
{
	uint32	id;
	dslist	*queue;
	HTAB	*conns;
	QueuePoolEntry *ent;
} Comm;

extern void CommInitialize(void);
extern Comm *CommCreate(uint32 id);
extern void CommAddConn(Comm *comm, uint32 id, const char *conninfo);
extern void CommDetach(Comm *comm);
extern void *CommPopRPCRequest(Comm *comm);
extern bool CommCallRPC(Comm *comm, RaftRPCRequest *rpc, List *entries, Term *term_p,
						bool *success_p);
extern bool CommCallRPCBegin(Comm *comm, RaftRPCRequest *rpc, List *entries);
extern bool CommCallRPCEnd(Comm *comm, uint32 to, Term *term_p, bool *success_p,
						   void callback(void));

#endif /* COMM_H */
