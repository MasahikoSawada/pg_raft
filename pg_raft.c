/*-------------------------------------------------------------------------
 *
 * pg_raft.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/time.h>

#include "access/heapam.h"
#include "catalog/namespace.h"
#include "catalog/indexing.h"
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

#include "comm.h"
#include "pg_raft.h"

PG_MODULE_MAGIC;

char *pg_raft_database = NULL;

PG_FUNCTION_INFO_V1(create_node);
PG_FUNCTION_INFO_V1(create_join_group);
PG_FUNCTION_INFO_V1(xx_add_remote_node);

void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
        elog(ERROR, "pg_raft is not in shared_preload_libraries");

	CommInitialize();

	DefineCustomStringVariable("pg_raft.database",
							   "Database to connect to.",
							   NULL,
							   &pg_raft_database,
							   "postgres",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("pg_raft");

	/* set up common data for our worker */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	sprintf(worker.bgw_library_name, "pg_raft");
	sprintf(worker.bgw_function_name, "pgr_worker_main");
	sprintf(worker.bgw_name, "pg_raft worker");
	worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&worker);
}

Oid
pgr_get_table_oid(const char *tablename, bool missing_ok)
{
	Oid nspOid, relOid;

	nspOid = get_namespace_oid(PG_RAFT_SCHEMA_NAME, missing_ok);
	if (!OidIsValid(nspOid))
	{
		if (missing_ok)
			return InvalidOid;

		elog(ERROR, "schema \"%s\" does not exist", PG_RAFT_SCHEMA_NAME);
	}

	relOid = get_relname_relid(tablename, nspOid);
	if (!OidIsValid(relOid))
	{
		if (missing_ok)
			return InvalidOid;

		elog(ERROR, "relation \"%s.%s\" does not exist",
			 PG_RAFT_SCHEMA_NAME, tablename);
	}

	return relOid;
}

static PgRaftNode *
pgr_get_node_from_tuple(Relation rel, HeapTuple tuple)
{
	PgRaftNode *node = NULL;
	TupleDesc tupdesc;
	bool isnull;

	if (!HeapTupleIsValid(tuple))
		return NULL;

	tupdesc = RelationGetDescr(rel);

	node = palloc(sizeof(PgRaftNode));
	node->id = DatumGetInt32(fastgetattr(tuple,
										 Anum_node_node_id,
										 tupdesc,
										 &isnull));
	node->name = text_to_cstring(
		DatumGetTextP(fastgetattr(tuple,
								  Anum_node_node_name,
								  tupdesc,
								  &isnull)));
	node->dns = text_to_cstring(
		DatumGetTextP(fastgetattr(tuple,
								  Anum_node_node_dns,
								  tupdesc,
								  &isnull)));
	node->group_id = DatumGetInt32(fastgetattr(tuple,
											   Anum_node_group_id,
											   tupdesc,
											   &isnull));
	node->is_local = DatumGetBool(fastgetattr(tuple,
											  Anum_node_is_local,
											  tupdesc,
											  &isnull));

	return node;
}

/*
 * Get all remote nodes in the given group.
 */
List *
pgr_get_all_remote_nodes(uint32 group_id)
{
	List *nodes = NIL;
	Oid relid;
	Relation rel;
	ScanKeyData skey[2];
	HeapTuple tuple;
	TableScanDesc scan;
	bool start_tx = false;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		start_tx = true;
	}

	relid = pgr_get_table_oid(TableName_node, true);
	if (!OidIsValid(relid))
		goto cleanup;

	rel = table_open(relid, AccessShareLock);

	ScanKeyInit(&skey[0],
				Anum_node_group_id,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(group_id));
	ScanKeyInit(&skey[1],
				Anum_node_is_local,
				BTEqualStrategyNumber, F_BOOLNE,
				BoolGetDatum(true));
	scan = table_beginscan_catalog(rel, 2, &skey[0]);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		PgRaftNode *node;

		node = pgr_get_node_from_tuple(rel, tuple);
		nodes = lappend(nodes, node);
	}

cleanup:
	table_endscan(scan);
	table_close(rel, NoLock);

	if (start_tx)
		CommitTransactionCommand();

	return nodes;
}

PgRaftNode *
pgr_get_local_node(void)
{
	PgRaftNode *node = NULL;
	Oid	relid;
	Relation rel;
	ScanKeyData skey;
	HeapTuple tuple;
	TableScanDesc scan;

	relid = pgr_get_table_oid(TableName_node, true);
	if (!OidIsValid(relid))
		return NULL;

	rel = table_open(relid, AccessShareLock);

	ScanKeyInit(&skey,
				Anum_node_is_local,
				BTEqualStrategyNumber, F_BOOLEQ,
				BoolGetDatum(true));
	scan = table_beginscan_catalog(rel, 1, &skey);

	tuple = heap_getnext(scan, ForwardScanDirection);
	if (!HeapTupleIsValid(tuple))
		goto cleanup;

	node = pgr_get_node_from_tuple(rel, tuple);

cleanup:
	table_endscan(scan);
	table_close(rel, NoLock);

	return node;
}

/*
 * Write the initial node state to the table. This must be called only once when
 * starting up for the first time.
 */
void
pgr_initialize_node_state(void)
{
	Relation rel;
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[Nattrs_node_state];
	bool nulls[Nattrs_node_state];

	rel = table_open(pgr_get_table_oid(TableName_node_state, false), RowExclusiveLock);
	tupdesc = RelationGetDescr(rel);

	MemSet(nulls, false, sizeof(nulls));
	values[Anum_node_state_current_term - 1] = Int64GetDatum(1);
	values[Anum_node_state_voted_for - 1] = Int32GetDatum(InvalidNodeId);

	/* create a new heap tuple and insert */
	tuple = heap_form_tuple(tupdesc, values, nulls);
	CatalogTupleInsert(rel, tuple);

	CacheInvalidateRelcache(rel);

	/* cleanup */
	heap_freetuple(tuple);
	table_close(rel, NoLock);
}

/*
 * Get the persistent node state: the current term and voted-for, from the table.
 */
bool
pgr_get_node_state(Term *term_p, NodeId *voted_for_p)
{
	Relation rel;
	TupleDesc tupdesc;
	HeapTuple tuple;
	TableScanDesc scan;
	bool isnull;

	rel = table_open(pgr_get_table_oid(TableName_node_state, false), AccessShareLock);
	tupdesc = RelationGetDescr(rel);

	scan = table_beginscan_catalog(rel, 0, NULL);
	tuple = heap_getnext(scan, ForwardScanDirection);
	if (!HeapTupleIsValid(tuple))
	{
		table_endscan(scan);
		table_close(rel, NoLock);
		return false;
	}

	*term_p = DatumGetInt64(fastgetattr(tuple,
										Anum_node_state_current_term,
										tupdesc,
										&isnull));
	Assert(!isnull);
	*voted_for_p = DatumGetInt32(fastgetattr(tuple,
											 Anum_node_state_voted_for,
											 tupdesc,
											 &isnull));
	Assert(!isnull);

	heap_freetuple(tuple);
	table_endscan(scan);
	table_close(rel, NoLock);

	return true;
}

void
pgr_update_node_state(Term term, NodeId voted_for)
{
	Relation rel;
	TupleDesc tupdesc;
	HeapTuple tuple;
	HeapTuple new_tuple;
	TableScanDesc scan;
	bool start_tx = false;
	Datum values[Nattrs_node_state];
	bool nulls[Nattrs_node_state] = {false};
	bool replaces[Nattrs_node_state] = {true};

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		start_tx = true;
	}

	rel = table_open(pgr_get_table_oid(TableName_node_state, false), RowExclusiveLock);
	tupdesc = RelationGetDescr(rel);

	scan = table_beginscan_catalog(rel, 0, NULL);
	tuple = heap_getnext(scan, ForwardScanDirection);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find the local node");

	MemSet(nulls, false, sizeof(nulls));
	values[Anum_node_state_current_term - 1] = Int64GetDatum(term);
	values[Anum_node_state_voted_for - 1] = Int32GetDatum(voted_for);

	/* create a new heap tuple and insert */
	new_tuple = heap_modify_tuple(tuple, tupdesc, values, nulls, replaces);
	CatalogTupleUpdate(rel, &tuple->t_self, new_tuple);
	CacheInvalidateRelcache(rel);

	/* cleanup */
	heap_freetuple(new_tuple);
	table_endscan(scan);
	table_close(rel, NoLock);

	if (start_tx)
		CommitTransactionCommand();
}

/* Set committed = true whose index <= 'upto' */
void
pgr_commit_log_entries(LogIndex upto)
{
	StringInfoData query;
	bool start_tx = false;
	int ret;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		start_tx = true;
	}

	initStringInfo(&query);
	appendStringInfo(&query,
					 "UPDATE pgraft._log_entry_raw SET committed = true WHERE NOT committed AND index <= %lu",
					 upto);

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_exec(query.data, 0);
	Assert(ret == SPI_OK_UPDATE);

	PopActiveSnapshot();
	SPI_finish();

	if (start_tx)
		CommitTransactionCommand();
}

void
pgr_append_log_entry(Term term, LogIndex index, text *data)
{
	StringInfoData query;
	bool start_tx = false;
	int ret;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		start_tx = true;
	}

	initStringInfo(&query);
	appendStringInfo(&query,
					 "INSERT INTO pgraft._log_entry_raw(term, index, data, committed) "
					 "VALUES(%lu, %lu, '%s', false) "
					 "ON CONFLICT (term, index) "
					 "DO UPDATE SET data = EXCLUDED.data",
					 term, index, text_to_cstring(data));

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_exec(query.data, 0);
	Assert(ret == SPI_OK_INSERT);

	PopActiveSnapshot();
	SPI_finish();

	if (start_tx)
		CommitTransactionCommand();
}

static void
pgr_create_node(NodeId id, char *name, char *dns, uint32 group_id, bool is_local)
{
	Relation rel;
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[Nattrs_node];
	bool nulls[Nattrs_node];

	if (is_local)
	{
		PgRaftNode *node = pgr_get_local_node();

		if (is_local && node)
			ereport(ERROR, (errmsg("local node already exists")));
	}


	rel = table_open(pgr_get_table_oid(TableName_node, false), RowExclusiveLock);
	tupdesc = RelationGetDescr(rel);

	MemSet(nulls, false, sizeof(nulls));
	values[Anum_node_node_id - 1] = Int32GetDatum(id);
	values[Anum_node_node_name - 1] = CStringGetTextDatum(name);
	values[Anum_node_node_dns - 1] = CStringGetTextDatum(dns);
	values[Anum_node_group_id - 1] = Int32GetDatum(group_id);
	values[Anum_node_is_local - 1 ] = BoolGetDatum(is_local);

	/* create a new heap tuple and insert */
	tuple = heap_form_tuple(tupdesc, values, nulls);
	CatalogTupleInsert(rel, tuple);

	CacheInvalidateRelcache(rel);

	/* cleanup */
	heap_freetuple(tuple);
	table_close(rel, NoLock);
}

Datum
create_node(PG_FUNCTION_ARGS)
{
	char *node_name = text_to_cstring(PG_GETARG_TEXT_P(0));
	char *node_dns = text_to_cstring(PG_GETARG_TEXT_P(1));
	NodeId	nodeid;
	struct timeval tv;
	BackgroundWorker bgw;
	BackgroundWorkerHandle *bgw_handle;

	/*
	gettimeofday(&tv, NULL);
	nodeid = ((uint32) tv.tv_sec) << 12;
	nodeid |= MyProcPid & 0xFFFF;
	*/
	nodeid = MyProcPid;

	pgr_create_node(nodeid, node_name, node_dns, 0, true);

	CommandCounterIncrement();

	/* Start a raft worker */
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "pg_raft");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "pgr_worker_main");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "pg_raft worker");
	bgw.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	bgw.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&bgw, &bgw_handle))
		ereport(WARNING, (errmsg("out of background worker slots")));

	PG_RETURN_VOID();
}

Datum
create_join_group(PG_FUNCTION_ARGS)
{
	char *group_name = text_to_cstring(PG_GETARG_TEXT_P(0));
	uint32 group_id = PG_ARGISNULL(1) ? GetNewObjectId() : PG_GETARG_INT32(1);
	bool create_only = PG_GETARG_BOOL(2);
	Relation rel;
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[Nattrs_node_group];
	bool nulls[Nattrs_node_group];

	/* XXX: group existence check */

	rel = table_open(pgr_get_table_oid(TableName_node_group, false), RowExclusiveLock);
	tupdesc = RelationGetDescr(rel);

	MemSet(nulls, false, sizeof(nulls));
	values[Anum_node_group_group_id - 1] = Int32GetDatum(group_id);
	values[Anum_node_group_group_name - 1] = CStringGetTextDatum(group_name);

	/* create a new heap tuple and insert */
	tuple = heap_form_tuple(tupdesc, values, nulls);
	CatalogTupleInsert(rel, tuple);

	/* cleanup */
	heap_freetuple(tuple);
	table_close(rel, NoLock);

	/* update the local node's group id as well */
	if (!create_only)
	{
		ScanKeyData skey;
		TableScanDesc scan;
		HeapTuple new_tuple;
		Datum node_values[Nattrs_node];
		bool node_nulls[Nattrs_node];
		bool node_replaces[Nattrs_node];

		rel = table_open(pgr_get_table_oid(TableName_node, false), RowExclusiveLock);

		ScanKeyInit(&skey,
					Anum_node_is_local,
					BTEqualStrategyNumber, F_BOOLEQ,
					BoolGetDatum(true));
		scan = table_beginscan_catalog(rel, 1, &skey);

		tuple = heap_getnext(scan, ForwardScanDirection);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "could not find the local node");

		MemSet(node_values, 0, sizeof(node_values));
		MemSet(node_nulls, false, sizeof(node_nulls));
		MemSet(node_replaces, false, sizeof(node_replaces));

		node_values[Anum_node_group_id - 1] = Int32GetDatum(group_id);
		node_replaces[Anum_node_group_id - 1] = true;

		new_tuple = heap_modify_tuple(tuple, RelationGetDescr(rel),
									  node_values, node_nulls, node_replaces);
		CatalogTupleUpdate(rel, &tuple->t_self, new_tuple);
		CacheInvalidateRelcache(rel);

		table_endscan(scan);
		table_close(rel, NoLock);
	}

	PG_RETURN_VOID();
}

Datum
xx_add_remote_node(PG_FUNCTION_ARGS)
{
	int node_id = PG_GETARG_INT32(0);
	char *node_name = text_to_cstring(PG_GETARG_TEXT_P(1));
	char *node_dns = text_to_cstring(PG_GETARG_TEXT_P(2));
	int	group_id = PG_GETARG_INT32(3);

	pgr_create_node(node_id, node_name, node_dns, group_id, false);

	PG_RETURN_VOID();
}
