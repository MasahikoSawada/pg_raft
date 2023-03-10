# ToDo
* [ ] launcher process.
* [x] network communication.
  * [x] add comm module.
  * [x] test on local.
* [ ] node management (drop, join, etc).
  * [x] create node.
  * [x] create group.
  * [x] add test code to add directly remote node.
  * [ ] get all nodes in the same group.
  * [ ] add conninfo to comm.
  * [ ] test heart beating.
* [ ] raft implementation.
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

