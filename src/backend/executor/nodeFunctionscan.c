/*-------------------------------------------------------------------------
 *
 * nodeFunctionscan.c
 *	  Support routines for scanning RangeFunctions (functions in rangetable).
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeFunctionscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecFunctionScan		scans a function.
 *		ExecFunctionNext		retrieve next tuple in sequential order.
 *		ExecInitFunctionScan	creates and initializes a functionscan node.
 *		ExecEndFunctionScan		releases any storage allocated.
 *		ExecReScanFunctionScan	rescans the function
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/nodeFunctionscan.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/memutils.h"


/*
 * Runtime data for each function being scanned.
 */
typedef struct FunctionScanPerFuncState {
    SetExprState *setexpr;        /* state of the expression being evaluated */
    TupleDesc tupdesc;        /* desc of the function result type */
    int colcount;        /* expected number of result columns */
    Tuplestorestate *tstore;    /* holds the function result set */
    int64 rowcount;        /* # of rows in result set, -1 if not known */
    TupleTableSlot *func_slot;    /* function result slot (or NULL) */
} FunctionScanPerFuncState;

static TupleTableSlot *FunctionNext(FunctionScanState *functionScanState);


/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		FunctionNext
 *
 *		This is a workhorse for ExecFunctionScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *FunctionNext(FunctionScanState *functionScanState) {
    // get information from the estate and scan state
    EState *estate = functionScanState->ss.ps.state;
    ScanDirection scanDirection = estate->es_direction;
    TupleTableSlot *tupleTableSlot = functionScanState->ss.ss_ScanTupleSlot;

    if (functionScanState->simple) {
        /*
         * Fast path for the trivial case: the function return type and scan
         * result type are the same, so we fetch the function result straight
         * into the scan result slot. No need to update ordinality or
         * rowcounts either.
         */
        Tuplestorestate *tuplestorestate = functionScanState->funcstates[0].tstore;

        // If first time through, read all tuples from function and put them
        // in a tuplestore. Subsequent calls just fetch tuples from tuplestore.
        if (tuplestorestate == NULL) {

            // 内部会去使用extension眼熟的fcinfo 把你的自定函数给驱动起来
            // 你在自定义函数中生成的tuplestorestate 注入到了 fcinfo->rsinfo.setResult 然后是该函数的返回的值
            functionScanState->funcstates[0].tstore = tuplestorestate =
                    ExecMakeTableFunctionResult(functionScanState->funcstates[0].setexpr,
                                                functionScanState->ss.ps.ps_ExprContext,
                                                functionScanState->argcontext,
                                                functionScanState->funcstates[0].tupdesc,
                                                functionScanState->eflags & EXEC_FLAG_BACKWARD);

            /*
             * paranoia - cope if the function, which may have constructed the
             * tuplestore itself, didn't leave it pointing at the start. This
             * call is fast, so the overhead shouldn't be an issue.
             */
            tuplestore_rescan(tuplestorestate);
        }

        // Get the next tuple from tuplestore.
        (void) tuplestore_gettupleslot(tuplestorestate,
                                       ScanDirectionIsForward(scanDirection),
                                       false,
                                       tupleTableSlot);
        return tupleTableSlot;
    }

    /*
     * Increment or decrement ordinal counter before checking for end-of-data,
     * so that we can move off either end of the result by 1 (and no more than
     * 1) without losing correct count.  See PortalRunSelect for why we can
     * assume that we won't be called repeatedly in the end-of-data state.
     */
    int64 oldpos = functionScanState->ordinal;
    if (ScanDirectionIsForward(scanDirection))
        functionScanState->ordinal++;
    else
        functionScanState->ordinal--;

    /*
     * Main loop over functions.
     *
     * We fetch the function results into func_slots (which match the function
     * return types), and then copy the values to scanslot (which matches the
     * scan result type), setting the ordinal column (if any) as well.
     */
    ExecClearTuple(tupleTableSlot);
    int att = 0;
    bool alldone = true;
    for (int funcno = 0; funcno < functionScanState->nfuncs; funcno++) {
        FunctionScanPerFuncState *fs = &functionScanState->funcstates[funcno];
        int i;

        /*
         * If first time through, read all tuples from function and put them
         * in a tuplestore. Subsequent calls just fetch tuples from
         * tuplestore.
         */
        if (fs->tstore == NULL) {
            fs->tstore = ExecMakeTableFunctionResult(fs->setexpr,
                                                     functionScanState->ss.ps.ps_ExprContext,
                                                     functionScanState->argcontext,
                                                     fs->tupdesc,
                                                     functionScanState->eflags & EXEC_FLAG_BACKWARD);

            /*
             * paranoia - cope if the function, which may have constructed the
             * tuplestore itself, didn't leave it pointing at the start. This
             * call is fast, so the overhead shouldn't be an issue.
             */
            tuplestore_rescan(fs->tstore);
        }

        /*
         * Get the next tuple from tuplestore.
         *
         * If we have a rowcount for the function, and we know the previous
         * read position was out of bounds, don't try the read. This allows
         * backward scan to work when there are mixed row counts present.
         */
        if (fs->rowcount != -1 && fs->rowcount < oldpos)
            ExecClearTuple(fs->func_slot);
        else
            (void) tuplestore_gettupleslot(fs->tstore,
                                           ScanDirectionIsForward(scanDirection),
                                           false,
                                           fs->func_slot);

        if (TupIsNull(fs->func_slot)) {
            /*
             * If we ran out of data for this function in the forward
             * direction then we now know how many rows it returned. We need
             * to know this in order to handle backwards scans. The row count
             * we store is actually 1+ the actual number, because we have to
             * position the tuplestore 1 off its end sometimes.
             */
            if (ScanDirectionIsForward(scanDirection) && fs->rowcount == -1)
                fs->rowcount = functionScanState->ordinal;

            /*
             * populate the result cols with nulls
             */
            for (i = 0; i < fs->colcount; i++) {
                tupleTableSlot->tts_values[att] = (Datum) 0;
                tupleTableSlot->tts_isnull[att] = true;
                att++;
            }
        } else {
            /*
             * we have a result, so just copy it to the result cols.
             */
            slot_getallattrs(fs->func_slot);

            for (i = 0; i < fs->colcount; i++) {
                tupleTableSlot->tts_values[att] = fs->func_slot->tts_values[i];
                tupleTableSlot->tts_isnull[att] = fs->func_slot->tts_isnull[i];
                att++;
            }

            /*
             * We're not done until every function result is exhausted; we pad
             * the shorter results with nulls until then.
             */
            alldone = false;
        }
    }

    // ordinal col is always last, per spec.
    if (functionScanState->ordinality) {
        tupleTableSlot->tts_values[att] = Int64GetDatumFast(functionScanState->ordinal);
        tupleTableSlot->tts_isnull[att] = false;
    }

    // If alldone, we just return the previously-cleared scanslot.  Otherwise finish creating the virtual tuple.
    if (!alldone)
        ExecStoreVirtualTuple(tupleTableSlot);

    return tupleTableSlot;
}

/*
 * FunctionRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
FunctionRecheck(FunctionScanState *node, TupleTableSlot *slot) {
    /* nothing to check */
    return true;
}

/* ----------------------------------------------------------------
 *		ExecFunctionScan(node)
 *
 *		Scans the function sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *ExecFunctionScan(PlanState *planState) {
    FunctionScanState *functionScanState = castNode(FunctionScanState, planState);

    return ExecScan(&functionScanState->ss, (ExecScanAccessMtd) FunctionNext, (ExecScanRecheckMtd) FunctionRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitFunctionScan
 * ----------------------------------------------------------------
 */
FunctionScanState *ExecInitFunctionScan(FunctionScan *functionScan, EState *estate, int eflags) {
    int nfuncs = list_length(functionScan->functions);

    int i, natts;
    ListCell *lc;

    /* check for unsupported flags */
    Assert(!(eflags & EXEC_FLAG_MARK));

    /*
     * FunctionScan should not have any children.
     */
    Assert(outerPlan(functionScan) == NULL);
    Assert(innerPlan(functionScan) == NULL);

    // create new
    FunctionScanState *functionScanState = makeNode(FunctionScanState);
    functionScanState->ss.ps.plan = (Plan *) functionScan;
    functionScanState->ss.ps.state = estate;
    functionScanState->ss.ps.ExecProcNode = ExecFunctionScan;
    functionScanState->eflags = eflags;

    /*
     * are we adding an ordinality column?
     */
    functionScanState->ordinality = functionScan->funcordinality;

    functionScanState->nfuncs = nfuncs;
    if (nfuncs == 1 && !functionScan->funcordinality)
        functionScanState->simple = true;
    else
        functionScanState->simple = false;

    /*
     * Ordinal 0 represents the "before the first row" position.
     *
     * We need to track ordinal position even when not adding an ordinality
     * column to the result, in order to handle backwards scanning properly
     * with multiple functions with different result sizes. (We can't position
     * any individual function's tuplestore any more than 1 place beyond its
     * end, so when scanning backwards, we need to know when to start
     * including the function in the scan again.)
     */
    functionScanState->ordinal = 0;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &functionScanState->ss.ps);

    functionScanState->funcstates = palloc(nfuncs * sizeof(FunctionScanPerFuncState));

    natts = 0;
    i = 0;
    foreach(lc, functionScan->functions) {
        RangeTblFunction *rangeTblFunction = (RangeTblFunction *) lfirst(lc);
        Node *funcexpr = rangeTblFunction->funcexpr;
        int colcount = rangeTblFunction->funccolcount;
        FunctionScanPerFuncState *fs = &functionScanState->funcstates[i];
        TypeFuncClass functypclass;
        Oid funcrettype;
        TupleDesc tupdesc;

        fs->setexpr = ExecInitTableFunctionResult((Expr *) funcexpr,
                                                  functionScanState->ss.ps.ps_ExprContext,
                                                  &functionScanState->ss.ps);

        /*
         * Don't allocate the tuplestores; the actual calls to the functions
         * do that.  NULL means that we have not called the function yet (or
         * need to call it again after a rescan).
         */
        fs->tstore = NULL;
        fs->rowcount = -1;

        /*
         * Now determine if the function returns a simple or composite type,
         * and build an appropriate tupdesc.  Note that in the composite case,
         * the function may now return more columns than it did when the plan
         * was made; we have to ignore any columns beyond "colcount".
         */
        functypclass = get_expr_result_type(funcexpr, &funcrettype, &tupdesc);

        if (functypclass == TYPEFUNC_COMPOSITE || functypclass == TYPEFUNC_COMPOSITE_DOMAIN) {
            /* Composite data type, e.g. a table's row type */
            Assert(tupdesc);
            Assert(tupdesc->natts >= colcount);
            /* Must copy it out of typcache for safety */
            tupdesc = CreateTupleDescCopy(tupdesc);
        } else if (functypclass == TYPEFUNC_SCALAR) {
            /* Base data type, i.e. scalar */
            tupdesc = CreateTemplateTupleDesc(1);
            TupleDescInitEntry(tupdesc,
                               (AttrNumber) 1,
                               NULL,    /* don't care about the name here */
                               funcrettype,
                               -1,
                               0);
            TupleDescInitEntryCollation(tupdesc,
                                        (AttrNumber) 1,
                                        exprCollation(funcexpr));
        } else if (functypclass == TYPEFUNC_RECORD) {
            tupdesc = BuildDescFromLists(rangeTblFunction->funccolnames,
                                         rangeTblFunction->funccoltypes,
                                         rangeTblFunction->funccoltypmods,
                                         rangeTblFunction->funccolcollations);

            /*
             * For RECORD results, make sure a typmod has been assigned.  (The
             * function should do this for itself, but let's cover things in
             * case it doesn't.)
             */
            BlessTupleDesc(tupdesc);
        } else {
            /* crummy error message, but parser should have caught this */
            elog(ERROR, "function in FROM has unsupported return type");
        }

        fs->tupdesc = tupdesc;
        fs->colcount = colcount;

        /*
         * We only need separate slots for the function results if we are
         * doing ordinality or multiple functions; otherwise, we'll fetch
         * function results directly into the scan slot.
         */
        if (!functionScanState->simple) {
            fs->func_slot = ExecInitExtraTupleSlot(estate, fs->tupdesc, &TTSOpsMinimalTuple);
        } else
            fs->func_slot = NULL;

        natts += colcount;
        i++;
    }

    /*
     * Create the combined TupleDesc
     *
     * If there is just one function without ordinality, the scan result
     * tupdesc is the same as the function result tupdesc --- except that we
     * may stuff new names into it below, so drop any rowtype label.
     */
    TupleDesc scan_tupdesc;
    if (functionScanState->simple) {
        scan_tupdesc = CreateTupleDescCopy(functionScanState->funcstates[0].tupdesc);
        scan_tupdesc->tdtypeid = RECORDOID;
        scan_tupdesc->tdtypmod = -1;
    } else {
        AttrNumber attno = 0;

        if (functionScan->funcordinality)
            natts++;

        scan_tupdesc = CreateTemplateTupleDesc(natts);

        for (i = 0; i < nfuncs; i++) {
            TupleDesc tupdesc = functionScanState->funcstates[i].tupdesc;
            int colcount = functionScanState->funcstates[i].colcount;
            int j;

            for (j = 1; j <= colcount; j++)
                TupleDescCopyEntry(scan_tupdesc, ++attno, tupdesc, j);
        }

        /* If doing ordinality, add a column of type "bigint" at the end */
        if (functionScan->funcordinality) {
            TupleDescInitEntry(scan_tupdesc,
                               ++attno,
                               NULL,    /* don't care about the name here */
                               INT8OID,
                               -1,
                               0);
        }

        Assert(attno == natts);
    }

    // initialize scan slot and type.
    ExecInitScanTupleSlot(estate, &functionScanState->ss, scan_tupdesc, &TTSOpsMinimalTuple);

    // initialize result slot, type and projection.
    ExecInitResultTypeTL(&functionScanState->ss.ps);
    ExecAssignScanProjectionInfo(&functionScanState->ss);

    // initialize child expressions
    functionScanState->ss.ps.qual = ExecInitQual(functionScan->scan.plan.qual, (PlanState *) functionScanState);

    /*
     * Create a memory context that ExecMakeTableFunctionResult can use to
     * evaluate function arguments in.  We can't use the per-tuple context for
     * this because it gets reset too often; but we don't want to leak
     * evaluation results into the query-lifespan context either.  We just
     * need one context, because we evaluate each function separately.
     */
    functionScanState->argcontext = AllocSetContextCreate(CurrentMemoryContext,
                                                          "Table function arguments",
                                                          ALLOCSET_DEFAULT_SIZES);

    return functionScanState;
}

/* ----------------------------------------------------------------
 *		ExecEndFunctionScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndFunctionScan(FunctionScanState *node) {
    int i;

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ss.ps);

    /*
     * clean out the tuple table
     */
    if (node->ss.ps.ps_ResultTupleSlot)
        ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * Release slots and tuplestore resources
     */
    for (i = 0; i < node->nfuncs; i++) {
        FunctionScanPerFuncState *fs = &node->funcstates[i];

        if (fs->func_slot)
            ExecClearTuple(fs->func_slot);

        if (fs->tstore != NULL) {
            tuplestore_end(node->funcstates[i].tstore);
            fs->tstore = NULL;
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecReScanFunctionScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanFunctionScan(FunctionScanState *node) {
    FunctionScan *scan = (FunctionScan *) node->ss.ps.plan;
    int i;
    Bitmapset *chgparam = node->ss.ps.chgParam;

    if (node->ss.ps.ps_ResultTupleSlot)
        ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    for (i = 0; i < node->nfuncs; i++) {
        FunctionScanPerFuncState *fs = &node->funcstates[i];

        if (fs->func_slot)
            ExecClearTuple(fs->func_slot);
    }

    ExecScanReScan(&node->ss);

    /*
     * Here we have a choice whether to drop the tuplestores (and recompute
     * the function outputs) or just rescan them.  We must recompute if an
     * expression contains changed parameters, else we rescan.
     *
     * XXX maybe we should recompute if the function is volatile?  But in
     * general the executor doesn't conditionalize its actions on that.
     */
    if (chgparam) {
        ListCell *lc;

        i = 0;
        foreach(lc, scan->functions) {
            RangeTblFunction *rtfunc = (RangeTblFunction *) lfirst(lc);

            if (bms_overlap(chgparam, rtfunc->funcparams)) {
                if (node->funcstates[i].tstore != NULL) {
                    tuplestore_end(node->funcstates[i].tstore);
                    node->funcstates[i].tstore = NULL;
                }
                node->funcstates[i].rowcount = -1;
            }
            i++;
        }
    }

    /* Reset ordinality counter */
    node->ordinal = 0;

    /* Make sure we rewind any remaining tuplestores */
    for (i = 0; i < node->nfuncs; i++) {
        if (node->funcstates[i].tstore != NULL)
            tuplestore_rescan(node->funcstates[i].tstore);
    }
}
