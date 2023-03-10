/* pg_raft */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_raft" to load this file. \quit

create table node (
node_id	int primary key,
node_name text,
node_dns text,
group_id int,
is_local boolean);
create index on node (is_local);

create table node_group (
group_id int primary key,
group_name text);

create table node_state (
current_term bigint,
voted_for int);

create table _log_entry_raw (
term int,
index bigint,
data text,
committed bool,
primary key(term, index));
create view log_entry as
    select term, index, data
    from pgraft._log_entry_raw
    where committed
    order by term, index;

create function create_node (
name text,
dns text)
returns void
as 'MODULE_PATHNAME'
language c strict parallel unsafe;

create function create_join_group (
group_name text,
group_id int default null,
create_only bool default false)
returns void
as 'MODULE_PATHNAME'
language c strict parallel unsafe;

create function request_vote_rpc (
in to_id bigint,
in from_id bigint,
in term bigint,
in candidate_id bigint,
in last_log_index bigint,
in last_log_term bigint,
out term bigint,
out vote_granted bool)
as 'MODULE_PATHNAME'
language c strict parallel unsafe;

create function append_entries_rpc (
in to_id bigint,
in from_id bigint,
in term bigint,
in leader_id bigint,
in prev_log_index bigint,
in prev_log_term bigint,
in leader_commit bigint,
VARIADIC entries text[] default NULL,
out term bigint,
out success bool)
as 'MODULE_PATHNAME'
language c parallel unsafe;

create function propose_message(
message text)
returns bool
as 'MODULE_PATHNAME'
language c strict parallel unsafe;

create function test_dslist()
returns void
as 'MODULE_PATHNAME'
language c strict;

create function xx_add_remote_node(
id int,
name text,
dns text,
group_id int)
returns void
as 'MODULE_PATHNAME'
language c strict parallel unsafe;

