/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  Definitions for using the POSTGRES copy command.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/copy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COPY_H
#define COPY_H

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"

// CopyStateData is private in commands/copy.c
typedef struct CopyStateData *CopyState;

typedef int (*copy_data_source_cb)(void *outbuf, int minread, int maxread);

extern void DoCopy(ParseState *state,
                   const CopyStmt *stmt,
                   int stmt_location, int stmt_len,
                   uint64 *processed);

extern void ProcessCopyOptions(ParseState *parseState,
                               CopyState copyState,
                               bool is_from,
                               List *optionList);

/*
 * setup to read tuples from a file for COPY FROM.
 *
 * 'relation': Used as a template for the tuples
 * 'filename': Name of server-local file to read
 * 'attrNameList': List of char *, columns to include. NIL selects all cols.
 * 'optionList': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyState, to be passed to NextCopyFrom and related functions.
 */
extern CopyState BeginCopyFrom(ParseState *parseState,
                               Relation relation,
                               const char *filename,
                               bool is_program,
                               copy_data_source_cb copyDataSourceCb,
                               List *attrNameList,
                               List *optionList);

extern void EndCopyFrom(CopyState copyState);

extern bool NextCopyFrom(CopyState copyState,
                         ExprContext *exprContext,
                         Datum *values,
                         bool *nulls);

extern bool NextCopyFromRawFields(CopyState copyState,
                                  char ***fields,
                                  int *nfields);

extern void CopyFromErrorCallback(void *arg);

extern uint64 CopyFrom(CopyState copyState);

extern DestReceiver *CreateCopyDestReceiver(void);

#endif                            /* COPY_H */
