/*-------------------------------------------------------------------------
 *
 * execScan.c
 *	  This code provides support for generalized relation scans. ExecScan
 *	  is passed a node and a pointer to a function to "do the right thing"
 *	  and return a tuple from the relation. ExecScan then does the tedious
 *	  stuff - checking the qualification and projecting the tuple
 *	  appropriately.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"



/*
 * ExecScanFetch -- check interrupts & fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.  If we aren't, just execute
 * the access method's next-tuple routine.
 */
static inline TupleTableSlot *ExecScanFetch(ScanState *scanState,
                                            ExecScanAccessMtd accessMethod,
                                            ExecScanRecheckMtd recheckMethod) {
    EState *estate = scanState->ps.state;

    CHECK_FOR_INTERRUPTS();

    if (estate->es_epq_active != NULL) {
        EPQState *epqstate = estate->es_epq_active;

        /*
         * We are inside an EvalPlanQual recheck.  Return the test tuple if
         * one is available, after rechecking any access-method-specific conditions.
         */
        Index scanRelId = ((Scan *) scanState->ps.plan)->scanrelid;

        if (scanRelId == 0) {
            /*
             * This is a ForeignScan or CustomScan which has pushed down a
             * join to the remote side.  The recheck method is responsible not
             * only for rechecking the scan/join quals but also for storing
             * the correct tuple in the slot.
             */

            TupleTableSlot *slot = scanState->ss_ScanTupleSlot;

            if (!(*recheckMethod)(scanState, slot))
                ExecClearTuple(slot);    /* would not be returned by scan */
            return slot;
        } else if (epqstate->relsubs_done[scanRelId - 1]) {
            /*
             * Return empty slot, as we already performed an EPQ substitution
             * for this relation.
             */

            TupleTableSlot *slot = scanState->ss_ScanTupleSlot;

            /* Return empty slot, as we already returned a tuple */
            return ExecClearTuple(slot);
        } else if (epqstate->relsubs_slot[scanRelId - 1] != NULL) {
            /*
             * Return replacement tuple provided by the EPQ caller.
             */

            TupleTableSlot *slot = epqstate->relsubs_slot[scanRelId - 1];

            Assert(epqstate->relsubs_rowmark[scanRelId - 1] == NULL);

            /* Mark to remember that we shouldn't return more */
            epqstate->relsubs_done[scanRelId - 1] = true;

            /* Return empty slot if we haven't got a test tuple */
            if (TupIsNull(slot))
                return NULL;

            /* Check if it meets the access-method conditions */
            if (!(*recheckMethod)(scanState, slot))
                return ExecClearTuple(slot);    /* would not be returned by
												 * scan */
            return slot;
        } else if (epqstate->relsubs_rowmark[scanRelId - 1] != NULL) {
            /*
             * Fetch and return replacement tuple using a non-locking rowmark.
             */

            TupleTableSlot *slot = scanState->ss_ScanTupleSlot;

            /* Mark to remember that we shouldn't return more */
            epqstate->relsubs_done[scanRelId - 1] = true;

            if (!EvalPlanQualFetchRowMark(epqstate, scanRelId, slot))
                return NULL;

            /* Return empty slot if we haven't got a test tuple */
            if (TupIsNull(slot))
                return NULL;

            /* Check if it meets the access-method conditions */
            if (!(*recheckMethod)(scanState, slot))
                return ExecClearTuple(slot);    /* would not be returned by
												 * scan */
            return slot;
        }
    }

    // Run the scanState-type-specific access method function to get the next tuple
    return (*accessMethod)(scanState);
}

/* ----------------------------------------------------------------
 *		ExecScan
 *
 *		Scans the relation using the 'access method' indicated and
 *		returns the next qualifying tuple in the direction specified
 *		in the global variable ExecDirection.
 *		The access method returns the next tuple and ExecScan() is
 *		responsible for checking the tuple returned against the qual-clause.
 *
 *		A 'recheck method' must also be provided that can check an
 *		arbitrary tuple of the relation against any qual conditions
 *		that are implemented internal to the access method.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *ExecScan(ScanState *scanState,
                         ExecScanAccessMtd accessMethod,    /* function returning a tuple */
                         ExecScanRecheckMtd recheckMethod) {
    ExprContext *exprContext;
    ExprState *exprState;
    ProjectionInfo *projectionInfo;

    // Fetch data from scanState
    exprState = scanState->ps.qual;
    projectionInfo = scanState->ps.ps_ProjInfo;
    exprContext = scanState->ps.ps_ExprContext;

    /* interrupt checks are in ExecScanFetch */

    /*
     * If we have neither a exprState to check nor a projection to do, just skip
     * all the overhead and return the raw scan tuple.
     */
    if (!exprState && !projectionInfo) {
        ResetExprContext(exprContext);
        return ExecScanFetch(scanState, accessMethod, recheckMethod);
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    ResetExprContext(exprContext);

    // get a tuple from the access method.  Loop until we obtain a tuple that passes the qualification.
    for (;;) {
        TupleTableSlot *tupleTableSlot = ExecScanFetch(scanState, accessMethod, recheckMethod);

        /*
         * if the tupleTableSlot returned by the accessMethod contains NULL, then it means
         * there is nothing more to scan so we just return an empty tupleTableSlot,
         * being careful to use the projection result tupleTableSlot so it has correct tupleDesc.
         */
        if (TupIsNull(tupleTableSlot)) {
            if (projectionInfo) {
                return ExecClearTuple(projectionInfo->pi_state.resultslot);
            }

            return tupleTableSlot;
        }

        // place the current tuple into the expr context
        exprContext->ecxt_scantuple = tupleTableSlot;

        /*
         * check that the current tuple satisfies the exprState-clause
         *
         * check for non-null exprState here to avoid a function call to ExecQual()
         * when the exprState is null ... saves only a few cycles, but they add up
         * ...
         */
        if (exprState == NULL || ExecQual(exprState, exprContext)) { // Found a satisfactory scan tuple.
            if (projectionInfo) { // Form a projection tuple, store it in the result tuple tupleTableSlot and return it.
                return ExecProject(projectionInfo);
            }

             // Here, we aren't projecting, so just return scan tuple.
             return tupleTableSlot;
        } else {
            InstrCountFiltered1(scanState, 1);
        }

        // Tuple fails exprState, so free per-tuple memory and try again.
        ResetExprContext(exprContext);
    }
}

/*
 * ExecAssignScanProjectionInfo
 *		Set up projection info for a scan node, if necessary.
 *
 * We can avoid a projection step if the requested tlist exactly matches
 * the underlying tuple type.  If so, we just set ps_ProjInfo to NULL.
 * Note that this case occurs not only for simple "SELECT * FROM ...", but
 * also in most cases where there are joins or other processing nodes above
 * the scan node, because the planner will preferentially generate a matching
 * tlist.
 *
 * The scan slot's descriptor must have been set already.
 */
void
ExecAssignScanProjectionInfo(ScanState *node)
{
	Scan	   *scan = (Scan *) node->ps.plan;
	TupleDesc	tupdesc = node->ss_ScanTupleSlot->tts_tupleDescriptor;

	ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, scan->scanrelid);
}

/*
 * ExecAssignScanProjectionInfoWithVarno
 *		As above, but caller can specify varno expected in Vars in the tlist.
 */
void
ExecAssignScanProjectionInfoWithVarno(ScanState *node, Index varno)
{
	TupleDesc	tupdesc = node->ss_ScanTupleSlot->tts_tupleDescriptor;

	ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, varno);
}

/*
 * ExecScanReScan
 *
 * This must be called within the ReScan function of any plan node type
 * that uses ExecScan().
 */
void
ExecScanReScan(ScanState *node)
{
	EState	   *estate = node->ps.state;

	/*
	 * We must clear the scan tuple so that observers (e.g., execCurrent.c)
	 * can tell that this plan node is not positioned on a tuple.
	 */
	ExecClearTuple(node->ss_ScanTupleSlot);

	/* Rescan EvalPlanQual tuple if we're inside an EvalPlanQual recheck */
	if (estate->es_epq_active != NULL)
	{
		EPQState   *epqstate = estate->es_epq_active;
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid > 0)
			epqstate->relsubs_done[scanrelid - 1] = false;
		else
		{
			Bitmapset  *relids;
			int			rtindex = -1;

			/*
			 * If an FDW or custom scan provider has replaced the join with a
			 * scan, there are multiple RTIs; reset the epqScanDone flag for
			 * all of them.
			 */
			if (IsA(node->ps.plan, ForeignScan))
				relids = ((ForeignScan *) node->ps.plan)->fs_relids;
			else if (IsA(node->ps.plan, CustomScan))
				relids = ((CustomScan *) node->ps.plan)->custom_relids;
			else
				elog(ERROR, "unexpected scan node: %d",
					 (int) nodeTag(node->ps.plan));

			while ((rtindex = bms_next_member(relids, rtindex)) >= 0)
			{
				Assert(rtindex > 0);
				epqstate->relsubs_done[rtindex - 1] = false;
			}
		}
	}
}
