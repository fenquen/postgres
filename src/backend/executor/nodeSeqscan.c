/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "utils/rel.h"

static TupleTableSlot *SeqNext(SeqScanState *seqScanState);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
// This is a workhorse for ExecSeqScan
static TupleTableSlot *SeqNext(SeqScanState *seqScanState) {
    // get information from the estate and scan state
    TableScanDesc tableScanDesc = seqScanState->ss.ss_currentScanDesc;
    EState *estate = seqScanState->ss.ps.state;
    ScanDirection scanDirection = estate->es_direction;
    TupleTableSlot *tupleTableSlot = seqScanState->ss.ss_ScanTupleSlot;

    if (tableScanDesc == NULL) { // 成立
        // We reach here if the scan is not parallel, or if we're serially executing a scan that was planned to be parallel.
        tableScanDesc = table_beginscan(seqScanState->ss.ss_currentRelation, estate->es_snapshot, 0, NULL);
        seqScanState->ss.ss_currentScanDesc = tableScanDesc;
    }

    // get the next tuple from the table
    if (table_scan_getnextslot(tableScanDesc, scanDirection, tupleTableSlot)) {
        return tupleTableSlot;
    }

    return NULL;
}

// access method routine to recheck a tuple in EvalPlanQual
static bool SeqRecheck(SeqScanState *node, TupleTableSlot *slot) {
    /*
     * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
     * (and this is very bad) - so, here we do not check are keys ok or not.
     */
    return true;
}

/* ---------------------------------------------------------------
 *		scan the relation sequentially and returns the next qualifying tuple.
 *		We call the ExecScan() routine and pass it the appropriate access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *ExecSeqScan(PlanState *planState) {
    SeqScanState *seqScanState = castNode(SeqScanState, planState);
    return ExecScan(&seqScanState->ss, (ExecScanAccessMtd) SeqNext, (ExecScanRecheckMtd) SeqRecheck);
}

// ExecInitSeqScan
SeqScanState *ExecInitSeqScan(SeqScan *seqScan, EState *estate, int eflags) {
    // once upon a time it was possible to have an outerPlan of a SeqScan, but not any more.
    Assert(outerPlan(seqScan) == NULL);
    Assert(innerPlan(seqScan) == NULL);

    SeqScanState *seqScanState = makeNode(SeqScanState);

    seqScanState->ss.ps.plan = (Plan *) seqScan;
    seqScanState->ss.ps.state = estate;
    seqScanState->ss.ps.ExecProcNode = ExecSeqScan;

    // Miscellaneous initialization
    // create expression context for node
    ExecAssignExprContext(estate, &seqScanState->ss.ps);

    // open the scan relation
    seqScanState->ss.ss_currentRelation = ExecOpenScanRelation(estate, seqScan->scanrelid, eflags);

    // 生成tupleTableSlot注入scanstate的ss_ScanTupleSlot
    ExecInitScanTupleSlot(estate,
                          &seqScanState->ss,
                          RelationGetDescr(seqScanState->ss.ss_currentRelation),
                          table_slot_callbacks(seqScanState->ss.ss_currentRelation));

    // Initialize result type and projection.
    ExecInitResultTypeTL(&seqScanState->ss.ps);
    ExecAssignScanProjectionInfo(&seqScanState->ss);

    // initialize child expressions
    seqScanState->ss.ps.qual = ExecInitQual(seqScan->plan.qual, (PlanState *) seqScanState);

    return seqScanState;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndSeqScan(SeqScanState *node) {
    TableScanDesc scanDesc;

    /*
     * get information from node
     */
    scanDesc = node->ss.ss_currentScanDesc;

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
     * close heap scan
     */
    if (scanDesc != NULL)
        table_endscan(scanDesc);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node) {
    TableScanDesc scan;

    scan = node->ss.ss_currentScanDesc;

    if (scan != NULL)
        table_rescan(scan,        /* scan desc */
                     NULL);        /* new scan keys */

    ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
                    ParallelContext *pcxt) {
    EState *estate = node->ss.ps.state;

    node->pscan_len = table_parallelscan_estimate(node->ss.ss_currentRelation,
                                                  estate->es_snapshot);
    shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
    shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
                         ParallelContext *pcxt) {
    EState *estate = node->ss.ps.state;
    ParallelTableScanDesc pscan;

    pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
    table_parallelscan_initialize(node->ss.ss_currentRelation,
                                  pscan,
                                  estate->es_snapshot);
    shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
    node->ss.ss_currentScanDesc =
            table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanReInitializeDSM(SeqScanState *node,
                           ParallelContext *pcxt) {
    ParallelTableScanDesc pscan;

    pscan = node->ss.ss_currentScanDesc->rs_parallel;
    table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node,
                            ParallelWorkerContext *pwcxt) {
    ParallelTableScanDesc pscan;

    pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
    node->ss.ss_currentScanDesc =
            table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}
