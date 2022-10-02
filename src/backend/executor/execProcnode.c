/*-------------------------------------------------------------------------
 *
 * execProcnode.c
 *	 contains dispatch functions which call the appropriate "initialize",
 *	 "get a tuple", and "cleanup" routines for the given node type.
 *	 If the node has children, then it will presumably call ExecInitNode,
 *	 ExecProcNode, or ExecEndNode on its subnodes and do the appropriate
 *	 processing.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execProcnode.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 NOTES
 *		This used to be three files.  It is now all combined into
 *		one file so that it is easier to keep the dispatch routines
 *		in sync when new nodes are added.
 *
 *	 EXAMPLE
 *		Suppose we want the age of the manager of the shoe department and
 *		the number of employees in that department.  So we have the query:
 *
 *				select DEPT.no_emps, EMP.age
 *				from DEPT, EMP
 *				where EMP.name = DEPT.mgr and
 *					  DEPT.name = "shoe"
 *
 *		Suppose the planner gives us the following plan:
 *
 *						Nest Loop (DEPT.mgr = EMP.name)
 *						/		\
 *					   /		 \
 *				   Seq Scan		Seq Scan
 *					DEPT		  EMP
 *				(name = "shoe")
 *
 *		ExecutorStart() is called first.
 *		It calls InitPlan() which calls ExecInitNode() on
 *		the root of the plan -- the nest loop node.
 *
 *	  * ExecInitNode() notices that it is looking at a nest loop and
 *		as the code below demonstrates, it calls ExecInitNestLoop().
 *		Eventually this calls ExecInitNode() on the right and left subplans
 *		and so forth until the entire plan is initialized.  The result
 *		of ExecInitNode() is a plan state tree built with the same structure
 *		as the underlying plan tree.
 *
 *	  * Then when ExecutorRun() is called, it calls ExecutePlan() which calls
 *		ExecProcNode() repeatedly on the top node of the plan state tree.
 *		Each time this happens, ExecProcNode() will end up calling
 *		ExecNestLoop(), which calls ExecProcNode() on its subplans.
 *		Each of these subplans is a sequential scan so ExecSeqScan() is
 *		called.  The slots returned by ExecSeqScan() may contain
 *		tuples which contain the attributes ExecNestLoop() uses to
 *		form the tuples it returns.
 *
 *	  * Eventually ExecSeqScan() stops returning tuples and the nest
 *		loop join ends.  Lastly, ExecutorEnd() calls ExecEndNode() which
 *		calls ExecEndNestLoop() which in turn calls ExecEndNode() on
 *		its subplans which result in ExecEndSeqScan().
 *
 *		This should show how the executor works by having
 *		ExecInitNode(), ExecProcNode() and ExecEndNode() dispatch
 *		their work to the appropriate node support routines which may
 *		in turn call these routines themselves on their subplans.
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "executor/nodeAppend.h"
#include "executor/nodeBitmapAnd.h"
#include "executor/nodeBitmapHeapscan.h"
#include "executor/nodeBitmapIndexscan.h"
#include "executor/nodeBitmapOr.h"
#include "executor/nodeCtescan.h"
#include "executor/nodeCustom.h"
#include "executor/nodeForeignscan.h"
#include "executor/nodeFunctionscan.h"
#include "executor/nodeGather.h"
#include "executor/nodeGatherMerge.h"
#include "executor/nodeGroup.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "executor/nodeIndexonlyscan.h"
#include "executor/nodeIndexscan.h"
#include "executor/nodeLimit.h"
#include "executor/nodeLockRows.h"
#include "executor/nodeMaterial.h"
#include "executor/nodeMergeAppend.h"
#include "executor/nodeMergejoin.h"
#include "executor/nodeModifyTable.h"
#include "executor/nodeNamedtuplestorescan.h"
#include "executor/nodeNestloop.h"
#include "executor/nodeProjectSet.h"
#include "executor/nodeRecursiveunion.h"
#include "executor/nodeResult.h"
#include "executor/nodeSamplescan.h"
#include "executor/nodeSeqscan.h"
#include "executor/nodeSetOp.h"
#include "executor/nodeSort.h"
#include "executor/nodeSubplan.h"
#include "executor/nodeSubqueryscan.h"
#include "executor/nodeTableFuncscan.h"
#include "executor/nodeTidscan.h"
#include "executor/nodeUnique.h"
#include "executor/nodeValuesscan.h"
#include "executor/nodeWindowAgg.h"
#include "executor/nodeWorktablescan.h"
#include "nodes/nodeFuncs.h"
#include "miscadmin.h"

static TupleTableSlot *ExecProcNodeFirst(PlanState *node);

static TupleTableSlot *ExecProcNodeInstr(PlanState *node);

/* ------------------------------------------------------------------------
 *		ExecInitNode 通过plan生成planState
 *
 *		Recursively initializes all the nodes in the plan tree rooted
 *		at 'node'.
 *
 *		Inputs:
 *		  'node' is the current node of the plan produced by the query planner
 *		  'estate' is the shared execution state for the plan tree
 *		  'eflags' is a bitwise OR of flag bits described in executor.h
 *
 *		Returns a PlanState node corresponding to the given Plan node.
 * ------------------------------------------------------------------------
 */
PlanState *ExecInitNode(Plan *plan, EState *estate, int eflags) {
    PlanState *planState;

    // do nothing when we get to the end of a leaf on tree.
    if (plan == NULL) {
        return NULL;
    }

    // to ensure the stack isn't overrun while initializing the node tree.
    check_stack_depth();

    switch (nodeTag(plan)) {
        case T_FunctionScan:
            planState = (PlanState *) ExecInitFunctionScan((FunctionScan *) plan, estate, eflags);
            break;
        case T_Result: // control nodes,insert对应的modifyTable的subPlan是它
            planState = (PlanState *) ExecInitResult((Result *) plan, estate, eflags);
            break;
        case T_ProjectSet:
            planState = (PlanState *) ExecInitProjectSet((ProjectSet *) plan, estate, eflags);
            break;
        case T_ModifyTable:
            planState = (PlanState *) ExecInitModifyTable((ModifyTable *) plan, estate, eflags);
            break;
        case T_Append:
            planState = (PlanState *) ExecInitAppend((Append *) plan, estate, eflags);
            break;
        case T_MergeAppend:
            planState = (PlanState *) ExecInitMergeAppend((MergeAppend *) plan, estate, eflags);
            break;
        case T_RecursiveUnion:
            planState = (PlanState *) ExecInitRecursiveUnion((RecursiveUnion *) plan, estate, eflags);
            break;
        case T_BitmapAnd:
            planState = (PlanState *) ExecInitBitmapAnd((BitmapAnd *) plan, estate, eflags);
            break;
        case T_BitmapOr:
            planState = (PlanState *) ExecInitBitmapOr((BitmapOr *) plan, estate, eflags);
            break;
            // scan nodes
        case T_SeqScan:
            planState = (PlanState *) ExecInitSeqScan((SeqScan *) plan, estate, eflags);
            break;
        case T_SampleScan:
            planState = (PlanState *) ExecInitSampleScan((SampleScan *) plan, estate, eflags);
            break;
        case T_IndexScan:
            planState = (PlanState *) ExecInitIndexScan((IndexScan *) plan, estate, eflags);
            break;
        case T_IndexOnlyScan:
            planState = (PlanState *) ExecInitIndexOnlyScan((IndexOnlyScan *) plan, estate, eflags);
            break;
        case T_BitmapIndexScan:
            planState = (PlanState *) ExecInitBitmapIndexScan((BitmapIndexScan *) plan, estate, eflags);
            break;

        case T_BitmapHeapScan:
            planState = (PlanState *) ExecInitBitmapHeapScan((BitmapHeapScan *) plan, estate, eflags);
            break;

        case T_TidScan:
            planState = (PlanState *) ExecInitTidScan((TidScan *) plan, estate, eflags);
            break;

        case T_SubqueryScan:
            planState = (PlanState *) ExecInitSubqueryScan((SubqueryScan *) plan,
                                                           estate, eflags);
            break;
        case T_TableFuncScan:
            planState = (PlanState *) ExecInitTableFuncScan((TableFuncScan *) plan, estate, eflags);
            break;
        case T_ValuesScan:
            planState = (PlanState *) ExecInitValuesScan((ValuesScan *) plan, estate, eflags);
            break;
        case T_CteScan:
            planState = (PlanState *) ExecInitCteScan((CteScan *) plan, estate, eflags);
            break;
        case T_NamedTuplestoreScan:
            planState = (PlanState *) ExecInitNamedTuplestoreScan((NamedTuplestoreScan *) plan, estate, eflags);
            break;
        case T_WorkTableScan:
            planState = (PlanState *) ExecInitWorkTableScan((WorkTableScan *) plan, estate, eflags);
            break;
        case T_ForeignScan:
            planState = (PlanState *) ExecInitForeignScan((ForeignScan *) plan, estate, eflags);
            break;
        case T_CustomScan:
            planState = (PlanState *) ExecInitCustomScan((CustomScan *) plan, estate, eflags);
            break;

            // join node
        case T_NestLoop:
            planState = (PlanState *) ExecInitNestLoop((NestLoop *) plan, estate, eflags);
            break;
        case T_MergeJoin:
            planState = (PlanState *) ExecInitMergeJoin((MergeJoin *) plan, estate, eflags);
            break;
        case T_HashJoin:
            planState = (PlanState *) ExecInitHashJoin((HashJoin *) plan, estate, eflags);
            break;

            // materialization node
        case T_Material:
            planState = (PlanState *) ExecInitMaterial((Material *) plan, estate, eflags);
            break;
        case T_Sort:
            planState = (PlanState *) ExecInitSort((Sort *) plan, estate, eflags);
            break;
        case T_Group:
            planState = (PlanState *) ExecInitGroup((Group *) plan, estate, eflags);
            break;
        case T_Agg:
            planState = (PlanState *) ExecInitAgg((Agg *) plan, estate, eflags);
            break;
        case T_WindowAgg:
            planState = (PlanState *) ExecInitWindowAgg((WindowAgg *) plan, estate, eflags);
            break;
        case T_Unique:
            planState = (PlanState *) ExecInitUnique((Unique *) plan, estate, eflags);
            break;
        case T_Gather:
            planState = (PlanState *) ExecInitGather((Gather *) plan, estate, eflags);
            break;
        case T_GatherMerge:
            planState = (PlanState *) ExecInitGatherMerge((GatherMerge *) plan, estate, eflags);
            break;
        case T_Hash:
            planState = (PlanState *) ExecInitHash((Hash *) plan, estate, eflags);
            break;
        case T_SetOp:
            planState = (PlanState *) ExecInitSetOp((SetOp *) plan, estate, eflags);
            break;
        case T_LockRows:
            planState = (PlanState *) ExecInitLockRows((LockRows *) plan, estate, eflags);
            break;
        case T_Limit:
            planState = (PlanState *) ExecInitLimit((Limit *) plan, estate, eflags);
            break;
        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(plan));
            planState = NULL;        /* keep compiler quiet */
            break;
    }

    ExecSetExecProcNode(planState, planState->ExecProcNode);

    // initialize any initPlans present in this node.  The planner put them in a separate list for us.
    List *subPlanList = NIL;
    ListCell *listCell;
    foreach(listCell, plan->initPlan) {
        SubPlan *subPlan = (SubPlan *) lfirst(listCell);

        Assert(IsA(subPlan, SubPlan));
        SubPlanState *subPlanState = ExecInitSubPlan(subPlan, planState);
        subPlanList = lappend(subPlanList, subPlanState);
    }
    planState->initPlan = subPlanList;

    // set up instrumentation for this node if requested */
    if (estate->es_instrument) {
        planState->instrument = InstrAlloc(1, estate->es_instrument);
    }

    return planState;
}


/*
 * If a node wants to change its ExecProcNode function after ExecInitNode()
 * has finished, it should do so with this function.  That way any wrapper
 * functions can be reinstalled, without the node having to know how that
 * works.
 */
void
ExecSetExecProcNode(PlanState *node, ExecProcNodeMtd function) {
    /*
     * Add a wrapper around the ExecProcNode callback that checks stack depth
     * during the first execution and maybe adds an instrumentation wrapper.
     * When the callback is changed after execution has already begun that
     * means we'll superfluously execute ExecProcNodeFirst, but that seems ok.
     */
    node->ExecProcNodeReal = function;
    node->ExecProcNode = ExecProcNodeFirst;
}


/*
 * ExecProcNode wrapper that performs some one-time checks, before calling
 * the relevant node method (possibly via an instrumentation wrapper).
 */
static TupleTableSlot *ExecProcNodeFirst(PlanState *node) {
    /*
     * Perform stack depth check during the first execution of the node.  We
     * only do so the first time round because it turns out to not be cheap on
     * some common architectures (eg. x86).  This relies on the assumption
     * that ExecProcNode calls for a given plan node will always be made at
     * roughly the same stack depth.
     */
    check_stack_depth();

    /*
     * If instrumentation is required, change the wrapper to one that just
     * does instrumentation.  Otherwise we can dispense with all wrappers and
     * have ExecProcNode() directly call the relevant function from now on.
     */
    if (node->instrument)
        node->ExecProcNode = ExecProcNodeInstr;
    else
        node->ExecProcNode = node->ExecProcNodeReal;

    return node->ExecProcNode(node);
}


/*
 * ExecProcNode wrapper that performs instrumentation calls.  By keeping
 * this a separate function, we avoid overhead in the normal case where
 * no instrumentation is wanted.
 */
static TupleTableSlot *
ExecProcNodeInstr(PlanState *node) {
    TupleTableSlot *result;

    InstrStartNode(node->instrument);

    result = node->ExecProcNodeReal(node);

    InstrStopNode(node->instrument, TupIsNull(result) ? 0.0 : 1.0);

    return result;
}


/* ----------------------------------------------------------------
 *		MultiExecProcNode
 *
 *		Execute a node that doesn't return individual tuples
 *		(it might return a hashtable, bitmap, etc).  Caller should
 *		check it got back the expected kind of Node.
 *
 * This has essentially the same responsibilities as ExecProcNode,
 * but it does not do InstrStartNode/InstrStopNode (mainly because
 * it can't tell how many returned tuples to count).  Each per-node
 * function must provide its own instrumentation support.
 * ----------------------------------------------------------------
 */
Node *MultiExecProcNode(PlanState *planState) {
    check_stack_depth();

    CHECK_FOR_INTERRUPTS();

    // something changed, let ReScan handle this
    if (planState->chgParam != NULL) {
        ExecReScan(planState);
    }

    Node *result;
    switch (nodeTag(planState)) {
        // only node types that actually support multi exec will be listed
        case T_HashState:
            result = MultiExecHash((HashState *) planState);
            break;
        case T_BitmapIndexScanState:
            result = MultiExecBitmapIndexScan((BitmapIndexScanState *) planState);
            break;
        case T_BitmapAndState:
            result = MultiExecBitmapAnd((BitmapAndState *) planState);
            break;
        case T_BitmapOrState:
            result = MultiExecBitmapOr((BitmapOrState *) planState);
            break;
        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(planState));
            result = NULL;
            break;
    }

    return result;
}


/* ----------------------------------------------------------------
 *		ExecEndNode
 *
 *		Recursively cleans up all the nodes in the plan rooted
 *		at 'node'.
 *
 *		After this operation, the query plan will not be able to be
 *		processed any further.  This should be called only after
 *		the query plan has been fully executed.
 * ----------------------------------------------------------------
 */
void
ExecEndNode(PlanState *node) {
    /*
     * do nothing when we get to the end of a leaf on tree.
     */
    if (node == NULL)
        return;

    /*
     * Make sure there's enough stack available. Need to check here, in
     * addition to ExecProcNode() (via ExecProcNodeFirst()), because it's not
     * guaranteed that ExecProcNode() is reached for all nodes.
     */
    check_stack_depth();

    if (node->chgParam != NULL) {
        bms_free(node->chgParam);
        node->chgParam = NULL;
    }

    switch (nodeTag(node)) {
        /*
         * control nodes
         */
        case T_ResultState:
            ExecEndResult((ResultState *) node);
            break;

        case T_ProjectSetState:
            ExecEndProjectSet((ProjectSetState *) node);
            break;

        case T_ModifyTableState:
            ExecEndModifyTable((ModifyTableState *) node);
            break;

        case T_AppendState:
            ExecEndAppend((AppendState *) node);
            break;

        case T_MergeAppendState:
            ExecEndMergeAppend((MergeAppendState *) node);
            break;

        case T_RecursiveUnionState:
            ExecEndRecursiveUnion((RecursiveUnionState *) node);
            break;

        case T_BitmapAndState:
            ExecEndBitmapAnd((BitmapAndState *) node);
            break;

        case T_BitmapOrState:
            ExecEndBitmapOr((BitmapOrState *) node);
            break;

            /*
             * scan nodes
             */
        case T_SeqScanState:
            ExecEndSeqScan((SeqScanState *) node);
            break;

        case T_SampleScanState:
            ExecEndSampleScan((SampleScanState *) node);
            break;

        case T_GatherState:
            ExecEndGather((GatherState *) node);
            break;

        case T_GatherMergeState:
            ExecEndGatherMerge((GatherMergeState *) node);
            break;

        case T_IndexScanState:
            ExecEndIndexScan((IndexScanState *) node);
            break;

        case T_IndexOnlyScanState:
            ExecEndIndexOnlyScan((IndexOnlyScanState *) node);
            break;

        case T_BitmapIndexScanState:
            ExecEndBitmapIndexScan((BitmapIndexScanState *) node);
            break;

        case T_BitmapHeapScanState:
            ExecEndBitmapHeapScan((BitmapHeapScanState *) node);
            break;

        case T_TidScanState:
            ExecEndTidScan((TidScanState *) node);
            break;

        case T_SubqueryScanState:
            ExecEndSubqueryScan((SubqueryScanState *) node);
            break;

        case T_FunctionScanState:
            ExecEndFunctionScan((FunctionScanState *) node);
            break;

        case T_TableFuncScanState:
            ExecEndTableFuncScan((TableFuncScanState *) node);
            break;

        case T_ValuesScanState:
            ExecEndValuesScan((ValuesScanState *) node);
            break;

        case T_CteScanState:
            ExecEndCteScan((CteScanState *) node);
            break;

        case T_NamedTuplestoreScanState:
            ExecEndNamedTuplestoreScan((NamedTuplestoreScanState *) node);
            break;

        case T_WorkTableScanState:
            ExecEndWorkTableScan((WorkTableScanState *) node);
            break;

        case T_ForeignScanState:
            ExecEndForeignScan((ForeignScanState *) node);
            break;

        case T_CustomScanState:
            ExecEndCustomScan((CustomScanState *) node);
            break;

            /*
             * join nodes
             */
        case T_NestLoopState:
            ExecEndNestLoop((NestLoopState *) node);
            break;

        case T_MergeJoinState:
            ExecEndMergeJoin((MergeJoinState *) node);
            break;

        case T_HashJoinState:
            ExecEndHashJoin((HashJoinState *) node);
            break;

            /*
             * materialization nodes
             */
        case T_MaterialState:
            ExecEndMaterial((MaterialState *) node);
            break;

        case T_SortState:
            ExecEndSort((SortState *) node);
            break;

        case T_GroupState:
            ExecEndGroup((GroupState *) node);
            break;

        case T_AggState:
            ExecEndAgg((AggState *) node);
            break;

        case T_WindowAggState:
            ExecEndWindowAgg((WindowAggState *) node);
            break;

        case T_UniqueState:
            ExecEndUnique((UniqueState *) node);
            break;

        case T_HashState:
            ExecEndHash((HashState *) node);
            break;

        case T_SetOpState:
            ExecEndSetOp((SetOpState *) node);
            break;

        case T_LockRowsState:
            ExecEndLockRows((LockRowsState *) node);
            break;

        case T_LimitState:
            ExecEndLimit((LimitState *) node);
            break;

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
            break;
    }
}

/*
 * ExecShutdownNode
 *
 * Give execution nodes a chance to stop asynchronous resource consumption
 * and release any resources still held.
 */
bool
ExecShutdownNode(PlanState *node) {
    if (node == NULL)
        return false;

    check_stack_depth();

    /*
     * Treat the node as running while we shut it down, but only if it's run
     * at least once already.  We don't expect much CPU consumption during
     * node shutdown, but in the case of Gather or Gather Merge, we may shut
     * down workers at this stage.  If so, their buffer usage will get
     * propagated into pgBufferUsage at this point, and we want to make sure
     * that it gets associated with the Gather node.  We skip this if the node
     * has never been executed, so as to avoid incorrectly making it appear
     * that it has.
     */
    if (node->instrument && node->instrument->running)
        InstrStartNode(node->instrument);

    planstate_tree_walker(node, ExecShutdownNode, NULL);

    switch (nodeTag(node)) {
        case T_GatherState:
            ExecShutdownGather((GatherState *) node);
            break;
        case T_ForeignScanState:
            ExecShutdownForeignScan((ForeignScanState *) node);
            break;
        case T_CustomScanState:
            ExecShutdownCustomScan((CustomScanState *) node);
            break;
        case T_GatherMergeState:
            ExecShutdownGatherMerge((GatherMergeState *) node);
            break;
        case T_HashState:
            ExecShutdownHash((HashState *) node);
            break;
        case T_HashJoinState:
            ExecShutdownHashJoin((HashJoinState *) node);
            break;
        default:
            break;
    }

    /* Stop the node if we started it above, reporting 0 tuples. */
    if (node->instrument && node->instrument->running)
        InstrStopNode(node->instrument, 0);

    return false;
}

/*
 * ExecSetTupleBound
 *
 * Set a tuple bound for a planstate node.  This lets child plan nodes
 * optimize based on the knowledge that the maximum number of tuples that
 * their parent will demand is limited.  The tuple bound for a node may
 * only be changed between scans (i.e., after node initialization or just
 * before an ExecReScan call).
 *
 * Any negative tuples_needed value means "no limit", which should be the
 * default assumption when this is not called at all for a particular node.
 *
 * Note: if this is called repeatedly on a plan tree, the exact same set
 * of nodes must be updated with the new limit each time; be careful that
 * only unchanging conditions are tested here.
 */
void
ExecSetTupleBound(int64 tuples_needed, PlanState *child_node) {
    /*
     * Since this function recurses, in principle we should check stack depth
     * here.  In practice, it's probably pointless since the earlier node
     * initialization tree traversal would surely have consumed more stack.
     */

    if (IsA(child_node, SortState)) {
        /*
         * If it is a Sort node, notify it that it can use bounded sort.
         *
         * Note: it is the responsibility of nodeSort.c to react properly to
         * changes of these parameters.  If we ever redesign this, it'd be a
         * good idea to integrate this signaling with the parameter-change
         * mechanism.
         */
        SortState *sortState = (SortState *) child_node;

        if (tuples_needed < 0) {
            /* make sure flag gets reset if needed upon rescan */
            sortState->bounded = false;
        } else {
            sortState->bounded = true;
            sortState->bound = tuples_needed;
        }
    } else if (IsA(child_node, AppendState)) {
        /*
         * If it is an Append, we can apply the bound to any nodes that are
         * children of the Append, since the Append surely need read no more
         * than that many tuples from any one input.
         */
        AppendState *aState = (AppendState *) child_node;
        int i;

        for (i = 0; i < aState->as_nplans; i++)
            ExecSetTupleBound(tuples_needed, aState->appendplans[i]);
    } else if (IsA(child_node, MergeAppendState)) {
        /*
         * If it is a MergeAppend, we can apply the bound to any nodes that
         * are children of the MergeAppend, since the MergeAppend surely need
         * read no more than that many tuples from any one input.
         */
        MergeAppendState *maState = (MergeAppendState *) child_node;
        int i;

        for (i = 0; i < maState->ms_nplans; i++)
            ExecSetTupleBound(tuples_needed, maState->mergeplans[i]);
    } else if (IsA(child_node, ResultState)) {
        /*
         * Similarly, for a projecting Result, we can apply the bound to its
         * child node.
         *
         * If Result supported qual checking, we'd have to punt on seeing a
         * qual.  Note that having a resconstantqual is not a showstopper: if
         * that condition succeeds it affects nothing, while if it fails, no
         * rows will be demanded from the Result child anyway.
         */
        if (outerPlanState(child_node))
            ExecSetTupleBound(tuples_needed, outerPlanState(child_node));
    } else if (IsA(child_node, SubqueryScanState)) {
        /*
         * We can also descend through SubqueryScan, but only if it has no
         * qual (otherwise it might discard rows).
         */
        SubqueryScanState *subqueryState = (SubqueryScanState *) child_node;

        if (subqueryState->ss.ps.qual == NULL)
            ExecSetTupleBound(tuples_needed, subqueryState->subplan);
    } else if (IsA(child_node, GatherState)) {
        /*
         * A Gather node can propagate the bound to its workers.  As with
         * MergeAppend, no one worker could possibly need to return more
         * tuples than the Gather itself needs to.
         *
         * Note: As with Sort, the Gather node is responsible for reacting
         * properly to changes to this parameter.
         */
        GatherState *gstate = (GatherState *) child_node;

        gstate->tuples_needed = tuples_needed;

        /* Also pass down the bound to our own copy of the child plan */
        ExecSetTupleBound(tuples_needed, outerPlanState(child_node));
    } else if (IsA(child_node, GatherMergeState)) {
        /* Same comments as for Gather */
        GatherMergeState *gstate = (GatherMergeState *) child_node;

        gstate->tuples_needed = tuples_needed;

        ExecSetTupleBound(tuples_needed, outerPlanState(child_node));
    }

    /*
     * In principle we could descend through any plan node type that is
     * certain not to discard or combine input rows; but on seeing a node that
     * can do that, we can't propagate the bound any further.  For the moment
     * it's unclear that any other cases are worth checking here.
     */
}
