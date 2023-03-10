#ifndef DSLIST_H
#define DSLIST_H

#include "utils/dsa.h"

/* control object, allocated in the shared memory */
typedef struct dslist_control
{
	dsa_pointer handle;

	LWLock lock;

	dsa_pointer head;
	dsa_pointer tail;
} dslist_control;

/* struct for a list element, allocated in the shared memory */
typedef struct dslist_elem
{
	dsa_pointer	next;
	Size size;
	char data[FLEXIBLE_ARRAY_MEMBER];
} dslist_elem;

/* per-backend struct for dslist */
typedef struct dslist
{
	dsa_area *area;
	dslist_control *control;
} dslist;

extern void dslist_initialize(void);
extern dslist *dslist_create(dsa_area *area);
extern dsa_pointer dslist_handle(dslist *dsl);
extern dslist *dslist_attach(dsa_area *area, dsa_pointer handle);
extern void dslist_detach(dslist *dsl);
extern void dslist_destroy(dslist *dsl);
extern void dslist_append(dslist *dsl, const void *data, Size size);
extern Size dslist_pop(dslist *dsl, char **data);
extern dslist_elem *dslist_pop_elem(dslist *dsl);
extern dslist_elem *dslist_append_elem(dslist *dsl, const void *data,
									   Size size, dsa_pointer *dp_p);

#endif /* DSLIST_H */
