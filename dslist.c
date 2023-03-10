/*-------------------------------------------------------------------------
 *
 * dslist.c
 *		singly-linked list backed by dynamic shared memory areas.
 *
 * This is designed for a simple linked list that can be used as a queue.
 * The support operations are only append and pop. Anyone attached to the
 * list can do that. Unlike shm_mq in the PostgreSQL, there is no limitation
 * on the number of readers and writers for the queue. A dslist is protected
 * by lwlock.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "dslist.h"

PG_FUNCTION_INFO_V1(test_dslist);

static int lwlock_tranche_id;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void
dslist_shmem_startup(void)
{
	lwlock_tranche_id = LWLockNewTrancheId();
	LWLockRegisterTranche(lwlock_tranche_id, "dslistLock");
}

void
dslist_initialize(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = dslist_shmem_startup;
}

static bool
dslist_is_empty(dslist *dsl)
{
	return !DsaPointerIsValid(dsl->control->head);
}

dslist *
dslist_create(dsa_area *area)
{
	dslist *dsl;
	dsa_pointer control;

	dsl = palloc(sizeof(dslist));

	control = dsa_allocate(area, sizeof(dslist_control));

	dsl->area = area;
	dsl->control = dsa_get_address(area, control);
	dsl->control->handle = control;
	LWLockInitialize(&dsl->control->lock, lwlock_tranche_id);
	dsl->control->head = InvalidDsaPointer;
	dsl->control->tail = InvalidDsaPointer;

	return dsl;
}

dsa_pointer
dslist_handle(dslist *dsl)
{
	return dsl->control->handle;
}

dslist *
dslist_attach(dsa_area *area, dsa_pointer handle)
{
	dslist *dsl;

	dsl = palloc(sizeof(dslist));

	dsl->area = area;
	dsl->control = dsa_get_address(area, handle);

	return dsl;
}

void
dslist_detach(dslist *dsl)
{
	pfree(dsl);
}

void
dslist_destroy(dslist *dsl)
{
	dsa_free(dsl->area, dsl->control->handle);
	pfree(dsl);
}

void
dslist_append(dslist *dsl, const void *data, Size size)
{
	Size total_size = MAXALIGN(sizeof(dslist_elem) + size);
	dsa_pointer dp;
	dslist_elem *elem;
	dslist_elem *tail;

	dp = dsa_allocate0(dsl->area, total_size);
	elem = dsa_get_address(dsl->area, dp);

	elem->next = InvalidDsaPointer;
	elem->size = size;
	memcpy(elem->data, data, size);

	LWLockAcquire(&dsl->control->lock, LW_EXCLUSIVE);

	if (dslist_is_empty(dsl))
	{
		dsl->control->head = dp;
		dsl->control->tail = dp;
		LWLockRelease(&dsl->control->lock);
		return;
	}

	/* append to the tail */
	tail = dsa_get_address(dsl->area, dsl->control->tail);
	tail->next = dp;
	dsl->control->tail = dp;

	LWLockRelease(&dsl->control->lock);
}

Size
dslist_pop(dslist *dsl, char **data)
{
	dsa_pointer head_dp;
	dslist_elem *head;
	Size sz;

	Assert(data != NULL);

	LWLockAcquire(&dsl->control->lock, LW_EXCLUSIVE);

	if (dslist_is_empty(dsl))
	{
		LWLockRelease(&dsl->control->lock);
		*data = NULL;
		return 0;
	}

	/* get the head */
	head_dp = dsl->control->head;
	head = dsa_get_address(dsl->area, head_dp);

	/* copy the contents to return */
	sz = head->size;
	*data = palloc(sz);
	memcpy(*data, head->data, sz);

	/* pop from the list */
	dsl->control->head = head->next;

	/* Adjust the tail as well if there is only one element in the list */
	if (dsl->control->tail == head_dp)
	{
		Assert(!DsaPointerIsValid(dsl->control->head));
		dsl->control->tail = InvalidDsaPointer;
	}

	/* free the old head */
	dsa_free(dsl->area, head_dp);

	LWLockRelease(&dsl->control->lock);

	return sz;
}

dslist_elem *
dslist_append_elem(dslist *dsl, const void *data, Size size,
				   dsa_pointer *dp_p)
{
	Size total_size = MAXALIGN(sizeof(dslist_elem) + size);
	dsa_pointer dp;
	dslist_elem *elem;
	dslist_elem *tail;

	dp = dsa_allocate0(dsl->area, total_size);
	elem = dsa_get_address(dsl->area, dp);

	elem->next = InvalidDsaPointer;
	elem->size = size;
	memcpy(elem->data, data, size);

	LWLockAcquire(&dsl->control->lock, LW_EXCLUSIVE);

	if (dslist_is_empty(dsl))
	{
		dsl->control->head = dp;
		dsl->control->tail = dp;
		LWLockRelease(&dsl->control->lock);

		*dp_p = dp;
		return elem;
	}

	/* append to the tail */
	tail = dsa_get_address(dsl->area, dsl->control->tail);
	tail->next = dp;
	dsl->control->tail = dp;

	LWLockRelease(&dsl->control->lock);

	*dp_p = dp;
	return elem;
}

dslist_elem *
dslist_pop_elem(dslist *dsl)
{
	dsa_pointer head_dp;
	dslist_elem *head;

	LWLockAcquire(&dsl->control->lock, LW_EXCLUSIVE);

	if (dslist_is_empty(dsl))
	{
		LWLockRelease(&dsl->control->lock);
		return NULL;
	}

	/* get the head */
	head_dp = dsl->control->head;
	head = dsa_get_address(dsl->area, head_dp);

	/* pop from the list */
	dsl->control->head = head->next;

	/* Adjust the tail as well if there is only one element in the list */
	if (dsl->control->tail == head_dp)
	{
		Assert(!DsaPointerIsValid(dsl->control->head));
		dsl->control->tail = InvalidDsaPointer;
	}

	LWLockRelease(&dsl->control->lock);

	return head;
}

static void
dslist_dump(dslist *dsl)
{
	dsa_pointer elem_dp;

	if (dslist_is_empty(dsl))
	{
		fprintf(stderr, "dslist is empty\n");
		return;
	}

	elem_dp = dsl->control->head;
	while (DsaPointerIsValid(elem_dp))
	{
		char hex_buf[1024];
		uint64 hex_len;
		dslist_elem *elem = dsa_get_address(dsl->area, elem_dp);

		hex_len = hex_encode(elem->data, elem->size, hex_buf);
		hex_buf[hex_len] = '\0';

		fprintf(stderr, "size = %lu data = %s\n", elem->size, hex_buf);

		elem_dp = elem->next;
	}
}

static void
_check(char *ret_data, int ret_size, char *expect, int expect_size)
{
	if (ret_size != expect_size)
		elog(ERROR, "dslist_pop() returned %d, expected %d",
			 ret_size, expect_size);
	if (memcmp(ret_data, expect, expect_size) != 0)
	{
		char rethex[1024] = {0};
		char exphex[1024] = {0};
		uint64 len1, len2;

		len1 = hex_encode(ret_data, ret_size, rethex);
		rethex[len1] = '\0';
		len2 = hex_encode(expect, expect_size, exphex);
		rethex[len2] = '\0';
		elog(ERROR, "dslist_pop() returned \"%s\", expected \"%s\"",
			 rethex, exphex);
	}
}

Datum
test_dslist(PG_FUNCTION_ARGS)
{
	dsa_area *area;
	dslist *list;
	char *ret;
	Size sz;

	area = dsa_create(lwlock_tranche_id);
	dsa_pin_mapping(area);

	list = dslist_create(area);

	if (!dslist_is_empty(list))
		elog(ERROR, "dslist_is_empty() on a newly created dslist returns false");

	if (dslist_pop(list, &ret) != 0)
		elog(ERROR, "dslist_pop() on an empty dslist return non-0 data");

	dslist_append(list, "1", 2);
	dslist_append(list, "12345", 6);
	dslist_append(list, "1234567890", 11);
	dslist_dump(list);

	sz = dslist_pop(list, &ret);
	_check(ret, sz, "1", 2);
	sz = dslist_pop(list, &ret);
	_check(ret, sz, "12345", 6);
	sz = dslist_pop(list, &ret);
	_check(ret, sz, "1234567890", 11);

	if (!dslist_is_empty(list))
		elog(ERROR, "dslist_is_empty() on an empty dslist  returns false");

	dslist_append(list, "1", 2);
	dslist_append(list, "12345", 6);
	dslist_append(list, "1234567890", 11);

	sz = dslist_pop(list, &ret);
	_check(ret, sz, "1", 2);
	sz = dslist_pop(list, &ret);
	_check(ret, sz, "12345", 6);
	sz = dslist_pop(list, &ret);
	_check(ret, sz, "1234567890", 11);

	dslist_destroy(list);
	dsa_detach(area);

	PG_RETURN_VOID();
}
