# pg_raft/Makefile

MODULE_big = pg_raft
DATA = pg_raft--1.0.sql
OBJS = pg_raft.o worker.o comm.o dslist.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = pg_raft

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
