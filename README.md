# pg_raft (WIP)

Basic implementation of Raft for a cluster of PostgreSQL nodes.

**WARNING: pg_raft is in-progress, consider it experimental.**

# Installation

```
$ make USE_PGXS=1
$ sudo make USE_PGXS=1 install
```

# Installation (WIP)

```
$ vi $PGDATA/postgresql.conf
shared_preload_libraries = 'pg_raft'
pg_raft.database = 'postgres'
$ pg_ctl start
```

```
$ psql
=# create extension pg_raft;
=# select pgraft.create_node('node-1', 'port=5432 dbname=postgres');"
```

# ToDo
* [ ] launcher process.
* [x] network communication.
  * [x] add comm module.
  * [x] test on local.
* [x] node management
  * [x] create node.
  * [x] create group.
  * [x] add test code to add directly remote node.
  * [x] get all nodes in the same group.
  * [x] add conninfo to comm.
  * [x] test heart beating.
* [ ] raft implementation.
  * [x] leader election.
  * [ ] log replication.
  * [ ] join to group.
  * [ ] part from group.

# Memo
* [ ] ID consideration (node_id and group_id). How to generate unique ID? use OID? existence check?

# Design

## Communication between nodes.

```
Worker
  * Send the RPC request.
  * Comm (comm.c)
    * Convert request to SQL query.
    * Connect to a backend on the remote server.
    * Execute the query e.g., SELECT pgraft.append_entries_rpc(...);
  |        ^
  |        | (term, success)
  |        |
  | execute SELECT term, success FROM pgraft.append_entries_rpc(...);
  v        |
Backend
  * Execute the query, e.g., SELECT pgraft.append_entries_rpc(...);
  * Push RPC request into the (shared) queue.
  * Set the worker's latch.
  * Wait for the worker to execute the function.
  * Comm (comm.c)
    * shared queue backed by DSA.
  |
  | enqueue the request.
  |
  v
+-----+
|queue|
+-----+
  |
  | pop the request.
  |
  v
Worker
  * Pop the request from the queue.
  * Execute the function.
  * Set the result and let the backend know it.
```

