/*-------------------------------------------------------------------------
 *
 * spccache.h
 *	  Tablespace cache.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/spccache.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPCCACHE_H
#define SPCCACHE_H

void		get_tablespace_page_costs(Oid tablespaceOid, float8 *tablespaceRandomPageCost,
                                      float8 *tableSpaceSeqPageCost);
int			get_tablespace_io_concurrency(Oid spcid);

#endif							/* SPCCACHE_H */
