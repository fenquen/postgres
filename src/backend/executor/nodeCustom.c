/* ------------------------------------------------------------------------
 *
 * nodeCustom.c
 *		Routines to handle execution of custom scan node
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "executor/executor.h"
#include "executor/nodeCustom.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/plannodes.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static TupleTableSlot *ExecCustomScan(PlanState *planState);

CustomScanState *ExecInitCustomScan(CustomScan *cscan, EState *estate, int eflags) {
    /*
     * Allocate the CustomScanState object.  We let the custom scan provider
     * do the palloc, in case it wants to make a larger object that embeds
     * CustomScanState as the first field.  It must set the node tag and the
     * methods field correctly at this time.  Other standard fields should be set to zero.
     */
    // 调用 CreateCustomScanState() 生成 customScanState 该过程中各个extension有义务注入其methods
    CustomScanState *customScanState = castNode(CustomScanState, cscan->methods->CreateCustomScanState(cscan));

    // ensure flags is filled correctly
    customScanState->flags = cscan->flags;

    /* fill up fields of ScanState */
    customScanState->ss.ps.plan = &cscan->scan.plan;
    customScanState->ss.ps.state = estate;
    customScanState->ss.ps.ExecProcNode = ExecCustomScan;

    /* create expression context for node */
    ExecAssignExprContext(estate, &customScanState->ss.ps);

    // open the scan relation, if any
    Index scanrelid = cscan->scan.scanrelid;
    Relation scan_rel = NULL;
    if (scanrelid > 0) {
        scan_rel = ExecOpenScanRelation(estate, scanrelid, eflags);
        customScanState->ss.ss_currentRelation = scan_rel;
    }

    // Determine the scan tuple type.  If the custom scan provider provided a
    // target list describing the scan tuples, use that; else use base relation's rowtype.
    Index tlistvarno;
    if (cscan->custom_scan_tlist != NIL || scan_rel == NULL) {
        TupleDesc scan_tupdesc;

        scan_tupdesc = ExecTypeFromTL(cscan->custom_scan_tlist);
        ExecInitScanTupleSlot(estate, &customScanState->ss, scan_tupdesc, &TTSOpsVirtual);
        /* Node's targetlist will contain Vars with varno = INDEX_VAR */
        tlistvarno = INDEX_VAR;
    } else {
        ExecInitScanTupleSlot(estate, &customScanState->ss, RelationGetDescr(scan_rel), &TTSOpsVirtual);
        /* Node's targetlist will contain Vars with varno = scanrelid */
        tlistvarno = scanrelid;
    }

    // initialize result slot, type and projection.
    ExecInitResultTupleSlotTL(&customScanState->ss.ps, &TTSOpsVirtual);
    ExecAssignScanProjectionInfoWithVarno(&customScanState->ss, tlistvarno);

    // initialize child expressions
    customScanState->ss.ps.qual = ExecInitQual(cscan->scan.plan.qual, (PlanState *) customScanState);

    // 各个的extension有义务对小弟plan去调用ExecInitNode()生成其对应的planState 保存到customScanState的custom_ps
    // 小弟的plan的保存位置是自由的也可能在实现类的本体上,例如timescaledb的chunkDispatchState便把小弟plan保存在了subplan
    customScanState->methods->BeginCustomScan(customScanState, estate, eflags);

    return customScanState;
}

static TupleTableSlot *ExecCustomScan(PlanState *planState) {
    CustomScanState *customScanState = castNode(CustomScanState, planState);

    CHECK_FOR_INTERRUPTS();

    Assert(customScanState->methods->ExecCustomScan != NULL);
    return customScanState->methods->ExecCustomScan(customScanState);
}

void
ExecEndCustomScan(CustomScanState *node) {
    Assert(node->methods->EndCustomScan != NULL);
    node->methods->EndCustomScan(node);

    /* Free the exprcontext */
    ExecFreeExprContext(&node->ss.ps);

    /* Clean out the tuple table */
    ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    ExecClearTuple(node->ss.ss_ScanTupleSlot);
}

void
ExecReScanCustomScan(CustomScanState *node) {
    Assert(node->methods->ReScanCustomScan != NULL);
    node->methods->ReScanCustomScan(node);
}

void
ExecCustomMarkPos(CustomScanState *node) {
    if (!node->methods->MarkPosCustomScan)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("custom scan \"%s\" does not support MarkPos",
                               node->methods->CustomName)));
    node->methods->MarkPosCustomScan(node);
}

void
ExecCustomRestrPos(CustomScanState *node) {
    if (!node->methods->RestrPosCustomScan)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("custom scan \"%s\" does not support MarkPos",
                               node->methods->CustomName)));
    node->methods->RestrPosCustomScan(node);
}

void
ExecCustomScanEstimate(CustomScanState *node, ParallelContext *pcxt) {
    const CustomExecMethods *methods = node->methods;

    if (methods->EstimateDSMCustomScan) {
        node->pscan_len = methods->EstimateDSMCustomScan(node, pcxt);
        shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
        shm_toc_estimate_keys(&pcxt->estimator, 1);
    }
}

void
ExecCustomScanInitializeDSM(CustomScanState *node, ParallelContext *pcxt) {
    const CustomExecMethods *methods = node->methods;

    if (methods->InitializeDSMCustomScan) {
        int plan_node_id = node->ss.ps.plan->plan_node_id;
        void *coordinate;

        coordinate = shm_toc_allocate(pcxt->toc, node->pscan_len);
        methods->InitializeDSMCustomScan(node, pcxt, coordinate);
        shm_toc_insert(pcxt->toc, plan_node_id, coordinate);
    }
}

void
ExecCustomScanReInitializeDSM(CustomScanState *node, ParallelContext *pcxt) {
    const CustomExecMethods *methods = node->methods;

    if (methods->ReInitializeDSMCustomScan) {
        int plan_node_id = node->ss.ps.plan->plan_node_id;
        void *coordinate;

        coordinate = shm_toc_lookup(pcxt->toc, plan_node_id, false);
        methods->ReInitializeDSMCustomScan(node, pcxt, coordinate);
    }
}

void
ExecCustomScanInitializeWorker(CustomScanState *node,
                               ParallelWorkerContext *pwcxt) {
    const CustomExecMethods *methods = node->methods;

    if (methods->InitializeWorkerCustomScan) {
        int plan_node_id = node->ss.ps.plan->plan_node_id;
        void *coordinate;

        coordinate = shm_toc_lookup(pwcxt->toc, plan_node_id, false);
        methods->InitializeWorkerCustomScan(node, pwcxt->toc, coordinate);
    }
}

void
ExecShutdownCustomScan(CustomScanState *node) {
    const CustomExecMethods *methods = node->methods;

    if (methods->ShutdownCustomScan)
        methods->ShutdownCustomScan(node);
}
