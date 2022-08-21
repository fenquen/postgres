/*-------------------------------------------------------------------------
 *
 * tuplestore.c
 *	  Generalized routines for temporary tuple storage.
 *
 * This module handles temporary storage of tuples for purposes such
 * as Materialize nodes, hashjoin batch files, etc.  It is essentially
 * a dumbed-down version of tuplesort.c; it does no sorting of tuples
 * but can only store and regurgitate a sequence of tuples.  However,
 * because no sort is required, it is allowed to start reading the sequence
 * before it has all been written.  This is particularly useful for cursors,
 * because it allows random access within the already-scanned portion of
 * a query without having to process the underlying scan to completion.
 * Also, it is possible to support multiple independent read pointers.
 *
 * A temporary file is used to handle the data if it exceeds the
 * space limit specified by the caller.
 *
 * The (approximate) amount of memory allowed to the tuplestore is specified
 * in kilobytes by the caller.  We absorb tuples and simply store them in an
 * in-memory array as long as we haven't exceeded maxKBytes.  If we do exceed
 * maxKBytes, we dump all the tuples into a temp file and then read from that
 * when needed.
 *
 * Upon creation, a tuplestore supports a single read pointer, numbered 0.
 * Additional read pointers can be created using tuplestore_alloc_read_pointer.
 * Mark/restore behavior is supported by copying read pointers.
 *
 * When the caller requests backward-scan capability, we write the temp file
 * in a format that allows either forward or backward scan.  Otherwise, only
 * forward scan is allowed.  A request for backward scan must be made before
 * putting any tuples into the tuplestore.  Rewind is normally allowed but
 * can be turned off via tuplestore_set_eflags; turning off rewind for all
 * read pointers enables truncation of the tuplestore at the oldest read point
 * for minimal memory usage.  (The caller must explicitly call tuplestore_trim
 * at appropriate times for truncation to actually happen.)
 *
 * Note: in TSS_WRITEFILE state, the temp file's seek position is the
 * current write position, and the write-position variables in the tuplestore
 * aren't kept up to date.  Similarly, in TSS_READFILE state the temp file's
 * seek position is the active read pointer's position, and that read pointer
 * isn't kept up to date.  We update the appropriate variables using ftell()
 * before switching to the other state or activating a different read pointer.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/sort/tuplestore.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "access/htup_details.h"
#include "commands/tablespace.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "storage/buffile.h"
#include "utils/memutils.h"
#include "utils/resowner.h"


/*
 * Possible states of a Tuplestore object.  These denote the states that
 * persist between calls of Tuplestore routines.
 */
typedef enum {
    TSS_INMEM,                    /* 起始的时候内存显然是够用的 Tuples still fit in memory */
    TSS_WRITEFILE,                /* 添加的tuple越来越多内存不够了 Writing to temp file */
    TSS_READFILE                /* Reading from temp file */
} TupStoreStatus;

/*
 * State for a single read pointer.  If we are in state INMEM then all the
 * read pointers' "current" fields denote the read positions.  In state
 * WRITEFILE, the file/offset fields denote the read positions.  In state
 * READFILE, inactive read pointers have valid file/offset, but the active
 * read pointer implicitly has position equal to the temp file's seek position.
 *
 * Special case: if eof_reached is true, then the pointer's read position is
 * implicitly equal to the write position, and current/file/offset aren't
 * maintained.  This way we need not update all the read pointers each time
 * we write.
 */
typedef struct {
    int eflags;            /* capability flags */
    bool eof_reached;    /* read has reached EOF */
    int current;        /* next array index to read */
    int file;            /* temp file# */
    off_t offset;            /* byte offset in file */
} TSReadPointer;

// Private state of a Tuplestore operation.
struct Tuplestorestate {
    TupStoreStatus status;        /* enumerated value as shown above */
    int eflags;            /* capability flags (OR of pointers' flags) */
    bool backward;        /* store extra length words in file? */
    bool interXact;        /* keep open through transactions? */
    bool truncated;        /* tuplestore_trim has removed tuples? */
    int64 availMem;        /* remaining memory available, in bytes */
    int64 allowedMem;        /* 1般是对应global变量 work_mem(1024个的kb),total memory allowed in byte */
    int64 tuples;            /* number of tuples added */
    BufFile *myfile;            /* 因内存不足带来的临时文件 or NULL if none */
    MemoryContext context;        /* memory context for holding tuples */
    ResourceOwner resowner;        /* resowner for holding temp files */

    /*
     * 对应函数 copytup_heap
     *
     * These function pointers decouple the routines that must know what kind
     * of tuple we are handling from the routines that don't need to know it.
     * They are set up by the tuplestore_begin_xxx routines.
     *
     * (Although tuplestore.c currently only supports heap tuples, I've copied
     * this part of tuplesort.c so that extension to other kinds of objects
     * will be easy if it's ever needed.)
     *
     * Function to copy a supplied input tuple into palloc'd space. (NB: we
     * assume that a single pfree() is enough to release the tuple later, so
     * the representation must be "flat" in one palloc chunk.) state->availMem
     * must be decreased by the amount of space used.
     */
    void *(*copytup)(Tuplestorestate *state, void *tup);

    /*
     * 对应函数 writetup_heap
     *
     * Function to write a stored tuple onto tape.  The representation of the
     * tuple on tape need not be the same as it is in memory; requirements on
     * the tape representation are given below.  After writing the tuple,
     * pfree() it, and increase state->availMem by the amount of memory space
     * thereby released.
     */
    void (*writetup)(Tuplestorestate *state, void *tup);

    /*
     * 对应函数 readtup_heap
     *
     * Function to read a stored tuple from tape back into memory. 'len' is
     * the already-read length of the stored tuple.  Create and return a
     * palloc'd copy, and decrease state->availMem by the amount of memory
     * space consumed.
     */
    void *(*readtup)(Tuplestorestate *state, unsigned int len);

    /*
     * 保存在内存中的tuple
     * This array holds pointers to tuples in memory if we are in state INMEM.
     * In states WRITEFILE and READFILE it's not used.
     *
     * When memtupdeleted > 0, the first memtupdeleted pointers are already
     * released due to a tuplestore_trim() operation, but we haven't expended
     * the effort to slide the remaining pointers down.  These unused pointers
     * are set to NULL to catch any invalid accesses.  Note that memtupcount
     * includes the deleted pointers.
     */
    void **memtuples;        /* array of pointers to palloc'd tuples */
    int memtupdeleted;    /* the first N slots are currently unused */
    int memtupcount;    /* number of tuples currently present */
    int memtupsize;        /* 能够在内存中的栏位数量 allocated length of memtuples array */
    bool growmemtuples;    /* memtuples' growth still underway? */

    /*
     * These variables are used to keep track of the current positions.
     *
     * In state WRITEFILE, the current file seek position is the write point;
     * in state READFILE, the write position is remembered in writepos_xxx.
     * (The write position is the same as EOF, but since BufFileSeek doesn't
     * currently implement SEEK_END, we have to remember it explicitly.)
     */
    TSReadPointer *readptrs;    /* array of read pointers */
    int activeptr;        /* index of the active read pointer */
    int readptrcount;    /* number of pointers currently valid */
    int readptrsize;    /* allocated length of readptrs array */

    int writepos_file;    /* file# (valid if READFILE state) */
    off_t writepos_offset;    /* offset (valid if READFILE state) */
};

#define COPYTUP(state, tup)    ((*(state)->copytup) (state, tup))
// 指向函数指针 writetup_heap
#define WRITETUP(state, tup) ((*(state)->writetup) (state, tup))
#define READTUP(state, len)    ((*(state)->readtup) (state, len))
#define LACKMEM(state)        ((state)->availMem < 0)
#define USEMEM(state, amt)    ((state)->availMem -= (amt))
#define FREEMEM(state, amt)    ((state)->availMem += (amt))

/*--------------------
 *
 * NOTES about on-tape representation of tuples:
 *
 * We require the first "unsigned int" of a stored tuple to be the total size
 * on-tape of the tuple, including itself (so it is never zero).
 * The remainder of the stored tuple
 * may or may not match the in-memory representation of the tuple ---
 * any conversion needed is the job of the writetup and readtup routines.
 *
 * If state->backward is true, then the stored representation of
 * the tuple must be followed by another "unsigned int" that is a copy of the
 * length --- so the total tape space used is actually sizeof(unsigned int)
 * more than the stored length value.  This allows read-backwards.  When
 * state->backward is not set, the write/read routines may omit the extra
 * length word.
 *
 * writetup is expected to write both length words as well as the tuple
 * data.  When readtup is called, the tape is positioned just after the
 * front length word; readtup must read the tuple data and advance past
 * the back length word (if present).
 *
 * The write/read routines can make use of the tuple description data
 * stored in the Tuplestorestate record, if needed. They are also expected
 * to adjust state->availMem by the amount of memory space (not tape space!)
 * released or consumed.  There is no error return from either writetup
 * or readtup; they should ereport() on failure.
 *
 *
 * NOTES about memory consumption calculations:
 *
 * We count space allocated for tuples against the maxKBytes limit,
 * plus the space used by the variable-size array memtuples.
 * Fixed-size space (primarily the BufFile I/O buffer) is not counted.
 * We don't worry about the size of the read pointer array, either.
 *
 * Note that we count actual space used (as shown by GetMemoryChunkSpace)
 * rather than the originally-requested size.  This is important since
 * palloc can add substantial overhead.  It's not a complete answer since
 * we won't count any wasted space in palloc allocation blocks, but it's
 * a lot better than what we were doing before 7.3.
 *
 *--------------------
 */


static Tuplestorestate *tuplestore_begin_common(int eflags,
                                                bool interXact,
                                                int maxKBytes);

static void tuplestore_puttuple_common(Tuplestorestate *tuplestorestate, void *tuple);

static void dumptuples(Tuplestorestate *tuplestorestate);

static unsigned int getlen(Tuplestorestate *tuplestorestate, bool eofOK);

static void *copytup_heap(Tuplestorestate *tuplestorestate, void *tup);

static void writetup_heap(Tuplestorestate *tuplestorestate, void *tuple);

static void *readtup_heap(Tuplestorestate *tuplestorestate, unsigned int len);

/*
 *		tuplestore_begin_xxx
 *
 * Initialize for a tuple store operation.
 */
static Tuplestorestate *tuplestore_begin_common(int eflags, bool interXact, int maxKBytes) {
    Tuplestorestate *tupleStoreState = (Tuplestorestate *) palloc0(sizeof(Tuplestorestate));

    tupleStoreState->status = TSS_INMEM;
    tupleStoreState->eflags = eflags;
    tupleStoreState->interXact = interXact;
    tupleStoreState->truncated = false;
    tupleStoreState->allowedMem = maxKBytes * 1024L;
    tupleStoreState->availMem = tupleStoreState->allowedMem;
    tupleStoreState->myfile = NULL;
    tupleStoreState->context = CurrentMemoryContext;
    tupleStoreState->resowner = CurrentResourceOwner;

    tupleStoreState->memtupdeleted = 0;
    tupleStoreState->memtupcount = 0;
    tupleStoreState->tuples = 0;


    tupleStoreState->growmemtuples = true;
    // 确定了能够在内存上分配多少个栏位
    // Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD; see comments in grow_memtuples().
    tupleStoreState->memtupsize = Max(16384 / sizeof(void *), ALLOCSET_SEPARATE_THRESHOLD / sizeof(void *) + 1);
    tupleStoreState->memtuples = (void **) palloc(tupleStoreState->memtupsize * sizeof(void *));

    USEMEM(tupleStoreState, GetMemoryChunkSpace(tupleStoreState->memtuples));

    tupleStoreState->activeptr = 0;
    tupleStoreState->readptrcount = 1;
    tupleStoreState->readptrsize = 8;        /* arbitrary */
    tupleStoreState->readptrs = (TSReadPointer *) palloc(tupleStoreState->readptrsize * sizeof(TSReadPointer));

    tupleStoreState->readptrs[0].eflags = eflags;
    tupleStoreState->readptrs[0].eof_reached = false;
    tupleStoreState->readptrs[0].current = 0;

    return tupleStoreState;
}

/*
 * Create a new tuplestore; other types of tuple stores (other than
 * "heap" tuple stores, for heap tuples) are possible, but not presently
 * implemented.
 *
 * randomAccess: if true, both forward and backward accesses to the
 * tuple store are allowed.
 *
 * interXact: if true, the files used for on-disk storage persist beyond the
 * end of the current transaction.  NOTE: It's the caller's responsibility to
 * create such a tuplestore in a memory context and resource owner that will
 * also survive transaction boundaries, and to ensure the tuplestore is closed
 * when it's no longer wanted.
 *
 * maxKBytes: how much data to store in memory (any data beyond this
 * amount is paged to disk).  When in doubt, use work_mem.
 */
Tuplestorestate *tuplestore_begin_heap(bool randomAccess, bool interXact, int maxKBytes) {
    // This interpretation of the meaning of randomAccess is compatible with the pre-8.3 behavior of tuplestores.
    int eflags = randomAccess ? (EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND) : (EXEC_FLAG_REWIND);

    Tuplestorestate *tupleStoreState = tuplestore_begin_common(eflags, interXact, maxKBytes);

    tupleStoreState->copytup = copytup_heap;
    tupleStoreState->writetup = writetup_heap;
    tupleStoreState->readtup = readtup_heap;

    return tupleStoreState;
}

/*
 * tuplestore_set_eflags
 *
 * Set the capability flags for read pointer 0 at a finer grain than is
 * allowed by tuplestore_begin_xxx.  This must be called before inserting
 * any data into the tuplestore.
 *
 * eflags is a bitmask following the meanings used for executor node
 * startup flags (see executor.h).  tuplestore pays attention to these bits:
 *		EXEC_FLAG_REWIND		need rewind to start
 *		EXEC_FLAG_BACKWARD		need backward fetch
 * If tuplestore_set_eflags is not called, REWIND is allowed, and BACKWARD
 * is set per "randomAccess" in the tuplestore_begin_xxx call.
 *
 * NOTE: setting BACKWARD without REWIND means the pointer can read backwards,
 * but not further than the truncation point (the furthest-back read pointer
 * position at the time of the last tuplestore_trim call).
 */
void tuplestore_set_eflags(Tuplestorestate *state, int eflags) {
    int i;

    if (state->status != TSS_INMEM || state->memtupcount != 0)
        elog(ERROR, "too late to call tuplestore_set_eflags");

    state->readptrs[0].eflags = eflags;
    for (i = 1; i < state->readptrcount; i++)
        eflags |= state->readptrs[i].eflags;
    state->eflags = eflags;
}

/*
 * tuplestore_alloc_read_pointer - allocate another read pointer.
 *
 * Returns the pointer's index.
 *
 * The new pointer initially copies the position of read pointer 0.
 * It can have its own eflags, but if any data has been inserted into
 * the tuplestore, these eflags must not represent an increase in
 * requirements.
 */
int
tuplestore_alloc_read_pointer(Tuplestorestate *state, int eflags) {
    /* Check for possible increase of requirements */
    if (state->status != TSS_INMEM || state->memtupcount != 0) {
        if ((state->eflags | eflags) != state->eflags)
            elog(ERROR, "too late to require new tuplestore eflags");
    }

    /* Make room for another read pointer if needed */
    if (state->readptrcount >= state->readptrsize) {
        int newcnt = state->readptrsize * 2;

        state->readptrs = (TSReadPointer *)
                repalloc(state->readptrs, newcnt * sizeof(TSReadPointer));
        state->readptrsize = newcnt;
    }

    /* And set it up */
    state->readptrs[state->readptrcount] = state->readptrs[0];
    state->readptrs[state->readptrcount].eflags = eflags;

    state->eflags |= eflags;

    return state->readptrcount++;
}

/*
 * tuplestore_clear
 *
 *	Delete all the contents of a tuplestore, and reset its read pointers
 *	to the start.
 */
void
tuplestore_clear(Tuplestorestate *state) {
    int i;
    TSReadPointer *readptr;

    if (state->myfile)
        BufFileClose(state->myfile);
    state->myfile = NULL;
    if (state->memtuples) {
        for (i = state->memtupdeleted; i < state->memtupcount; i++) {
            FREEMEM(state, GetMemoryChunkSpace(state->memtuples[i]));
            pfree(state->memtuples[i]);
        }
    }
    state->status = TSS_INMEM;
    state->truncated = false;
    state->memtupdeleted = 0;
    state->memtupcount = 0;
    state->tuples = 0;
    readptr = state->readptrs;
    for (i = 0; i < state->readptrcount; readptr++, i++) {
        readptr->eof_reached = false;
        readptr->current = 0;
    }
}

/*
 * tuplestore_end
 *
 *	Release resources and clean up.
 */
void
tuplestore_end(Tuplestorestate *state) {
    int i;

    if (state->myfile)
        BufFileClose(state->myfile);
    if (state->memtuples) {
        for (i = state->memtupdeleted; i < state->memtupcount; i++)
            pfree(state->memtuples[i]);
        pfree(state->memtuples);
    }
    pfree(state->readptrs);
    pfree(state);
}

/*
 * tuplestore_select_read_pointer - make the specified read pointer active
 */
void
tuplestore_select_read_pointer(Tuplestorestate *state, int ptr) {
    TSReadPointer *readptr;
    TSReadPointer *oldptr;

    Assert(ptr >= 0 && ptr < state->readptrcount);

    /* No work if already active */
    if (ptr == state->activeptr)
        return;

    readptr = &state->readptrs[ptr];
    oldptr = &state->readptrs[state->activeptr];

    switch (state->status) {
        case TSS_INMEM:
        case TSS_WRITEFILE:
            /* no work */
            break;
        case TSS_READFILE:

            /*
             * First, save the current read position in the pointer about to
             * become inactive.
             */
            if (!oldptr->eof_reached)
                BufFileTell(state->myfile,
                            &oldptr->file,
                            &oldptr->offset);

            /*
             * We have to make the temp file's seek position equal to the
             * logical position of the new read pointer.  In eof_reached
             * state, that's the EOF, which we have available from the saved
             * write position.
             */
            if (readptr->eof_reached) {
                if (BufFileSeek(state->myfile,
                                state->writepos_file,
                                state->writepos_offset,
                                SEEK_SET) != 0)
                    ereport(ERROR,
                            (errcode_for_file_access(),
                                    errmsg("could not seek in tuplestore temporary file")));
            } else {
                if (BufFileSeek(state->myfile,
                                readptr->file,
                                readptr->offset,
                                SEEK_SET) != 0)
                    ereport(ERROR,
                            (errcode_for_file_access(),
                                    errmsg("could not seek in tuplestore temporary file")));
            }
            break;
        default:
            elog(ERROR, "invalid tuplestore state");
            break;
    }

    state->activeptr = ptr;
}

/*
 * tuplestore_tuple_count
 *
 * Returns the number of tuples added since creation or the last
 * tuplestore_clear().
 */
int64
tuplestore_tuple_count(Tuplestorestate *state) {
    return state->tuples;
}

/*
 * tuplestore_ateof
 *
 * Returns the active read pointer's eof_reached state.
 */
bool
tuplestore_ateof(Tuplestorestate *state) {
    return state->readptrs[state->activeptr].eof_reached;
}

/*
 * Grow the memtuples[] array, if possible within our memory constraint.  We
 * must not exceed INT_MAX tuples in memory or the caller-provided memory
 * limit.  Return true if we were able to enlarge the array, false if not.
 *
 * Normally, at each increment we double the size of the array.  When doing
 * that would exceed a limit, we attempt one last, smaller increase (and then
 * clear the growmemtuples flag so we don't try any more).  That allows us to
 * use memory as fully as permitted; sticking to the pure doubling rule could
 * result in almost half going unused.  Because availMem moves around with
 * tuple addition/removal, we need some rule to prevent making repeated small
 * increases in memtupsize, which would just be useless thrashing.  The
 * growmemtuples flag accomplishes that and also prevents useless
 * recalculations in this function.
 */
static bool grow_memtuples(Tuplestorestate *state) {
    int newmemtupsize;
    int memtupsize = state->memtupsize;
    int64 memNowUsed = state->allowedMem - state->availMem;

    /* Forget it if we've already maxed out memtuples, per comment above */
    if (!state->growmemtuples)
        return false;

    /* Select new value of memtupsize */
    if (memNowUsed <= state->availMem) {
        /*
         * We've used no more than half of allowedMem; double our usage,
         * clamping at INT_MAX tuples.
         */
        if (memtupsize < INT_MAX / 2)
            newmemtupsize = memtupsize * 2;
        else {
            newmemtupsize = INT_MAX;
            state->growmemtuples = false;
        }
    } else {
        /*
         * This will be the last increment of memtupsize.  Abandon doubling
         * strategy and instead increase as much as we safely can.
         *
         * To stay within allowedMem, we can't increase memtupsize by more
         * than availMem / sizeof(void *) elements. In practice, we want to
         * increase it by considerably less, because we need to leave some
         * space for the tuples to which the new array slots will refer.  We
         * assume the new tuples will be about the same size as the tuples
         * we've already seen, and thus we can extrapolate from the space
         * consumption so far to estimate an appropriate new size for the
         * memtuples array.  The optimal value might be higher or lower than
         * this estimate, but it's hard to know that in advance.  We again
         * clamp at INT_MAX tuples.
         *
         * This calculation is safe against enlarging the array so much that
         * LACKMEM becomes true, because the memory currently used includes
         * the present array; thus, there would be enough allowedMem for the
         * new array elements even if no other memory were currently used.
         *
         * We do the arithmetic in float8, because otherwise the product of
         * memtupsize and allowedMem could overflow.  Any inaccuracy in the
         * result should be insignificant; but even if we computed a
         * completely insane result, the checks below will prevent anything
         * really bad from happening.
         */
        double grow_ratio;

        grow_ratio = (double) state->allowedMem / (double) memNowUsed;
        if (memtupsize * grow_ratio < INT_MAX)
            newmemtupsize = (int) (memtupsize * grow_ratio);
        else
            newmemtupsize = INT_MAX;

        /* We won't make any further enlargement attempts */
        state->growmemtuples = false;
    }

    /* Must enlarge array by at least one element, else report failure */
    if (newmemtupsize <= memtupsize) {
        goto noalloc;
    }

    /*
     * On a 32-bit machine, allowedMem could exceed MaxAllocHugeSize.  Clamp
     * to ensure our request won't be rejected.  Note that we can easily
     * exhaust address space before facing this outcome.  (This is presently
     * impossible due to guc.c's MAX_KILOBYTES limitation on work_mem, but
     * don't rely on that at this distance.)
     */
    if ((Size) newmemtupsize >= MaxAllocHugeSize / sizeof(void *)) {
        newmemtupsize = (int) (MaxAllocHugeSize / sizeof(void *));
        state->growmemtuples = false;    /* can't grow any more */
    }

    /*
     * We need to be sure that we do not cause LACKMEM to become true, else
     * the space management algorithm will go nuts.  The code above should
     * never generate a dangerous request, but to be safe, check explicitly
     * that the array growth fits within availMem.  (We could still cause
     * LACKMEM if the memory chunk overhead associated with the memtuples
     * array were to increase.  That shouldn't happen because we chose the
     * initial array size large enough to ensure that palloc will be treating
     * both old and new arrays as separate chunks.  But we'll check LACKMEM
     * explicitly below just in case.)
     */
    if (state->availMem < (int64) ((newmemtupsize - memtupsize) * sizeof(void *))) {
        goto noalloc;
    }

    /* OK, do it */
    FREEMEM(state, GetMemoryChunkSpace(state->memtuples));
    state->memtupsize = newmemtupsize;
    state->memtuples = (void **) repalloc_huge(state->memtuples, state->memtupsize * sizeof(void *));
    USEMEM(state, GetMemoryChunkSpace(state->memtuples));

    if (LACKMEM(state)) {
        elog(ERROR, "unexpected out-of-memory situation in tuplestore");
    }

    return true;

    noalloc:
    /* If for any reason we didn't realloc, shut off future attempts */
    state->growmemtuples = false;
    return false;
}

/*
 * Accept one tuple and append it to the tuplestore.
 *
 * Note that the input tuple is always copied; the caller need not save it.
 *
 * If the active read pointer is currently "at EOF", it remains so (the read
 * pointer implicitly advances along with the write pointer); otherwise the
 * read pointer is unchanged.  Non-active read pointers do not move, which
 * means they are certain to not be "at EOF" immediately after puttuple.
 * This curious-seeming behavior is for the convenience of nodeMaterial.c and
 * nodeCtescan.c, which would otherwise need to do extra pointer repositioning
 * steps.
 *
 * tuplestore_puttupleslot() is a convenience routine to collect data from
 * a TupleTableSlot without an extra copy operation.
 */
void
tuplestore_puttupleslot(Tuplestorestate *state,
                        TupleTableSlot *slot) {
    MinimalTuple tuple;
    MemoryContext oldcxt = MemoryContextSwitchTo(state->context);

    /*
     * Form a MinimalTuple in working memory
     */
    tuple = ExecCopySlotMinimalTuple(slot);
    USEMEM(state, GetMemoryChunkSpace(tuple));

    tuplestore_puttuple_common(state, (void *) tuple);

    MemoryContextSwitchTo(oldcxt);
}

/*
 * "Standard" case to copy from a HeapTuple.  This is actually now somewhat
 * deprecated, but not worth getting rid of in view of the number of callers.
 */
void tuplestore_puttuple(Tuplestorestate *tuplestorestate, HeapTuple heapTuple) {
    MemoryContext old = MemoryContextSwitchTo(tuplestorestate->context);

    /*
     * Copy the tuple.  (Must do this even in WRITEFILE case.  Note that
     * COPYTUP includes USEMEM, so we needn't do that here.)
     */
    heapTuple = tuplestorestate->copytup(tuplestorestate, heapTuple);//COPYTUP(tuplestorestate, heapTuple);

    tuplestore_puttuple_common(tuplestorestate, (void *) heapTuple);

    MemoryContextSwitchTo(old);
}

/*
 * Similar to tuplestore_puttuple(), but work from values + nulls arrays.
 * This avoids an extra tuple-construction operation.
 */
void tuplestore_putvalues(Tuplestorestate *tuplestorestate,
                          TupleDesc tupleDesc,
                          Datum *values,
                          bool *isnull) {
    MemoryContext oldcxt = MemoryContextSwitchTo(tuplestorestate->context);

    MinimalTuple minimalTuple = heap_form_minimal_tuple(tupleDesc, values, isnull);

    USEMEM(tuplestorestate, GetMemoryChunkSpace(minimalTuple));

    tuplestore_puttuple_common(tuplestorestate, (void *) minimalTuple);

    MemoryContextSwitchTo(oldcxt);
}

static void tuplestore_puttuple_common(Tuplestorestate *tuplestorestate, void *tuple) {
    tuplestorestate->tuples++;

    switch (tuplestorestate->status) {
        case TSS_INMEM: {
            // Update read pointers as needed; see API spec above.
            TSReadPointer *tsReadPointer = tuplestorestate->readptrs;
            for (int i = 0; i < tuplestorestate->readptrcount; tsReadPointer++, i++) {
                if (tsReadPointer->eof_reached && i != tuplestorestate->activeptr) {
                    tsReadPointer->eof_reached = false;
                    tsReadPointer->current = tuplestorestate->memtupcount;
                }
            }

            /*
             * 因为栏位数量不足需要扩大
             * Grow the array as needed.  Note that we try to grow the array
             * when there is still one free slot remaining --- if we fail,
             * there'll still be room to store the incoming tuple, and then
             * we'll switch to tape-based operation.
             */
            if (tuplestorestate->memtupcount >= tuplestorestate->memtupsize - 1) {
                (void) grow_memtuples(tuplestorestate);
                Assert(tuplestorestate->memtupcount < tuplestorestate->memtupsize);
            }

            /* 保存 the tuple in the in-memory array */
            tuplestorestate->memtuples[tuplestorestate->memtupcount++] = tuple;

            // 要是内存上的栏位足够 && 内存足够
            if (tuplestorestate->memtupcount < tuplestorestate->memtupsize && !LACKMEM(tuplestorestate)) {
                return;
            }


            // 到了这里便是内存不够了需要落地到临时文件
            /*
             * Nope; time to switch to tape-based operation.  Make sure that
             * the temp file(s) are created in suitable temp tablespace.
             */
            PrepareTempTablespaces();

            /* associate the file with the store's resource owner */
            ResourceOwner resourceOwnerOld = CurrentResourceOwner;
            CurrentResourceOwner = tuplestorestate->resowner;

            // 临时的文件
            tuplestorestate->myfile = BufFileCreateTemp(tuplestorestate->interXact);

            CurrentResourceOwner = resourceOwnerOld;

            /*
             * Freeze the decision about whether trailing length words will be
             * used.  We can't change this choice once data is on tape, even
             * though callers might drop the requirement.
             */
            tuplestorestate->backward = (tuplestorestate->eflags & EXEC_FLAG_BACKWARD) != 0;
            tuplestorestate->status = TSS_WRITEFILE;

            dumptuples(tuplestorestate);
            break;
        }
        case TSS_WRITEFILE: {
            // Update read pointers as needed; see API spec above. Note:
            // BufFileTell is quite cheap, so not worth trying to avoid multiple calls.
            TSReadPointer *tsReadPointer = tuplestorestate->readptrs;
            for (int i = 0; i < tuplestorestate->readptrcount; tsReadPointer++, i++) {
                if (tsReadPointer->eof_reached && i != tuplestorestate->activeptr) {
                    tsReadPointer->eof_reached = false;
                    BufFileTell(tuplestorestate->myfile, &tsReadPointer->file,
                                &tsReadPointer->offset);
                }
            }

            WRITETUP(tuplestorestate, tuple);
            break;
        }
        case TSS_READFILE:
            // Switch from reading to writing.
            if (!tuplestorestate->readptrs[tuplestorestate->activeptr].eof_reached) {
                BufFileTell(tuplestorestate->myfile,
                            &tuplestorestate->readptrs[tuplestorestate->activeptr].file,
                            &tuplestorestate->readptrs[tuplestorestate->activeptr].offset);
            }

            if (BufFileSeek(tuplestorestate->myfile,
                            tuplestorestate->writepos_file,
                            tuplestorestate->writepos_offset,
                            SEEK_SET) != 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek in tuplestore temporary file")));
            }

            tuplestorestate->status = TSS_WRITEFILE;

            // update read pointers as needed; see API spec above.
            TSReadPointer *tsReadPointer = tuplestorestate->readptrs;
            for (int i = 0; i < tuplestorestate->readptrcount; tsReadPointer++, i++) {
                if (tsReadPointer->eof_reached && i != tuplestorestate->activeptr) {
                    tsReadPointer->eof_reached = false;
                    tsReadPointer->file = tuplestorestate->writepos_file;
                    tsReadPointer->offset = tuplestorestate->writepos_offset;
                }
            }

            WRITETUP(tuplestorestate, tuple);
            break;
        default:
            elog(ERROR, "invalid tuplestore state");
            break;
    }
}

/*
 * Fetch the next tuple in either forward or back direction.
 * Returns NULL if no more tuples.  If should_free is set, the
 * caller must pfree the returned tuple when done with it.
 *
 * Backward scan is only allowed if randomAccess was set true or
 * EXEC_FLAG_BACKWARD was specified to tuplestore_set_eflags().
 */
static void *tuplestore_gettuple(Tuplestorestate *tuplestorestate,
                                 bool forward,
                                 bool *should_free) {
    TSReadPointer *tsReaderPointer = &tuplestorestate->readptrs[tuplestorestate->activeptr];

    unsigned int tuplen;

    Assert(forward || (tsReaderPointer->eflags & EXEC_FLAG_BACKWARD));

    switch (tuplestorestate->status) {
        case TSS_INMEM:
            *should_free = false;
            if (forward) {
                if (tsReaderPointer->eof_reached)
                    return NULL;

                // We have another tuple, so return it
                if (tsReaderPointer->current < tuplestorestate->memtupcount) {
                    return tuplestorestate->memtuples[tsReaderPointer->current++];
                }

                tsReaderPointer->eof_reached = true;
                return NULL;
            }

            // if all tuples are fetched already then we return last
            // tuple, else tuple before last returned.
            if (tsReaderPointer->eof_reached) {
                tsReaderPointer->current = tuplestorestate->memtupcount;
                tsReaderPointer->eof_reached = false;
            } else {
                if (tsReaderPointer->current <= tuplestorestate->memtupdeleted) {
                    Assert(!tuplestorestate->truncated);
                    return NULL;
                }

                tsReaderPointer->current--; /* last returned tuple */
            }

            if (tsReaderPointer->current <= tuplestorestate->memtupdeleted) {
                Assert(!tuplestorestate->truncated);
                return NULL;
            }

            return tuplestorestate->memtuples[tsReaderPointer->current - 1];
        case TSS_WRITEFILE:
            /* skip state change if we'll just return NULL */
            if (tsReaderPointer->eof_reached && forward) {
                return NULL;
            }

            // switch from writing to reading.
            BufFileTell(tuplestorestate->myfile,
                        &tuplestorestate->writepos_file,
                        &tuplestorestate->writepos_offset);

            if (!tsReaderPointer->eof_reached) {
                if (BufFileSeek(tuplestorestate->myfile,
                                tsReaderPointer->file, tsReaderPointer->offset,
                                SEEK_SET) != 0) {
                    ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek in tuplestore temporary file")));
                }

            }
            tuplestorestate->status = TSS_READFILE;
            // fallthrough 由 write 变为 read
        case TSS_READFILE:
            *should_free = true;
            if (forward) {
                if ((tuplen = getlen(tuplestorestate, true)) != 0) {
                    return READTUP(tuplestorestate, tuplen);
                }

                tsReaderPointer->eof_reached = true;
                return NULL;
            }

            /*
             * backward.
             *
             * if all tuples are fetched already then we return last tuple, else tuple before last returned.
             *
             * Back up to fetch previously-returned tuple's ending length
             * word. If seek fails, assume we are at start of file.
             */

            /* even a failed backwards fetch gets you out of eof state */
            if (BufFileSeek(tuplestorestate->myfile, 0, -(long) sizeof(unsigned int), SEEK_CUR) != 0) {
                tsReaderPointer->eof_reached = false;
                Assert(!tuplestorestate->truncated);
                return NULL;
            }

            tuplen = getlen(tuplestorestate, false);

            if (tsReaderPointer->eof_reached) {
                // We will return the tuple returned before returning NULL
                tsReaderPointer->eof_reached = false;
            } else {
                // back up to get ending length word of tuple before it.
                if (BufFileSeek(tuplestorestate->myfile, 0,
                                -(long) (tuplen + 2 * sizeof(unsigned int)),
                                SEEK_CUR) != 0) {
                    /*
                     * If that fails, presumably the prev tuple is the first
                     * in the file.  Back up so that it becomes next to read
                     * in forward direction (not obviously right, but that is what in-memory case does).
                     */
                    if (BufFileSeek(tuplestorestate->myfile, 0,
                                    -(long) (tuplen + sizeof(unsigned int)),
                                    SEEK_CUR) != 0)
                        ereport(ERROR,
                                (errcode_for_file_access(), errmsg("could not seek in tuplestore temporary file")));
                    Assert(!tuplestorestate->truncated);
                    return NULL;
                }
                tuplen = getlen(tuplestorestate, false);
            }

            /*
             * Now we have the length of the prior tuple, back up and read it.
             * Note: READTUP expects we are positioned after the initial
             * length word of the tuple, so back up to that point.
             */
            if (BufFileSeek(tuplestorestate->myfile, 0, -(long) tuplen, SEEK_CUR) != 0) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not seek in tuplestore temporary file")));
            }

            return READTUP(tuplestorestate, tuplen);
        default:
            elog(ERROR, "invalid tuplestore state");
            return NULL;        /* keep compiler quiet */
    }
}

/*
 * exported function to fetch a MinimalTuple 1点点的到tuplestorestate去取数据需要外部的循环驱动
 *
 * If successful, put tuple in slot and return true; else, clear the slot and return false.
 *
 * If copy is true, the slot receives a copied tuple (allocated in current
 * memory context) that will stay valid regardless of future manipulations of
 * the tuplestore's state.  If copy is false, the slot may just receive a
 * pointer to a tuple held within the tuplestore.  The latter is more
 * efficient but the slot contents may be corrupted if additional writes to
 * the tuplestore occur.  (If using tuplestore_trim, see comments therein.)
 */
bool tuplestore_gettupleslot(Tuplestorestate *tuplestorestate,
                             bool forward,
                             bool copy,
                             TupleTableSlot *tupleTableSlot) {
    bool should_free;
    MinimalTuple minimalTuple = (MinimalTuple) tuplestore_gettuple(tuplestorestate, forward, &should_free);

    if (minimalTuple) {
        if (copy && !should_free) {
            minimalTuple = heap_copy_minimal_tuple(minimalTuple);
            should_free = true;
        }

        ExecStoreMinimalTuple(minimalTuple, tupleTableSlot, should_free);
        return true;
    }

    ExecClearTuple(tupleTableSlot);
    return false;
}

/*
 * tuplestore_advance - exported function to adjust position without fetching
 *
 * We could optimize this case to avoid palloc/pfree overhead, but for the
 * moment it doesn't seem worthwhile.
 */
bool
tuplestore_advance(Tuplestorestate *state, bool forward) {
    void *tuple;
    bool should_free;

    tuple = tuplestore_gettuple(state, forward, &should_free);

    if (tuple) {
        if (should_free)
            pfree(tuple);
        return true;
    } else {
        return false;
    }
}

/*
 * Advance over N tuples in either forward or back direction,
 * without returning any data.  N<=0 is a no-op.
 * Returns true if successful, false if ran out of tuples.
 */
bool
tuplestore_skiptuples(Tuplestorestate *state, int64 ntuples, bool forward) {
    TSReadPointer *readptr = &state->readptrs[state->activeptr];

    Assert(forward || (readptr->eflags & EXEC_FLAG_BACKWARD));

    if (ntuples <= 0)
        return true;

    switch (state->status) {
        case TSS_INMEM:
            if (forward) {
                if (readptr->eof_reached)
                    return false;
                if (state->memtupcount - readptr->current >= ntuples) {
                    readptr->current += ntuples;
                    return true;
                }
                readptr->current = state->memtupcount;
                readptr->eof_reached = true;
                return false;
            } else {
                if (readptr->eof_reached) {
                    readptr->current = state->memtupcount;
                    readptr->eof_reached = false;
                    ntuples--;
                }
                if (readptr->current - state->memtupdeleted > ntuples) {
                    readptr->current -= ntuples;
                    return true;
                }
                Assert(!state->truncated);
                readptr->current = state->memtupdeleted;
                return false;
            }
            break;

        default:
            /* We don't currently try hard to optimize other cases */
            while (ntuples-- > 0) {
                void *tuple;
                bool should_free;

                tuple = tuplestore_gettuple(state, forward, &should_free);

                if (tuple == NULL)
                    return false;
                if (should_free)
                    pfree(tuple);
                CHECK_FOR_INTERRUPTS();
            }
            return true;
    }
}

/*
 * move tuples from memory and write to tape 用来给内存省省把memtupdeleted个内存迁移到文件
 *
 * As a side effect, we must convert each read pointer's position from
 * "current" to file/offset format.  But eof_reached pointers don't
 * need to change state.
 */
static void dumptuples(Tuplestorestate *tuplestorestate) {

    for (int i = tuplestorestate->memtupdeleted;; i++) {
        TSReadPointer *readptr = tuplestorestate->readptrs;

        for (int j = 0; j < tuplestorestate->readptrcount; readptr++, j++) {
            if (i == readptr->current && !readptr->eof_reached) {
                BufFileTell(tuplestorestate->myfile, &readptr->file, &readptr->offset);
            }
        }

        if (i >= tuplestorestate->memtupcount) {
            break;
        }

        WRITETUP(tuplestorestate, tuplestorestate->memtuples[i]);
    }

    tuplestorestate->memtupdeleted = 0;
    tuplestorestate->memtupcount = 0;
}

/*
 * tuplestore_rescan		- rewind the active read pointer to start
 */
void
tuplestore_rescan(Tuplestorestate *state) {
    TSReadPointer *readptr = &state->readptrs[state->activeptr];

    Assert(readptr->eflags & EXEC_FLAG_REWIND);
    Assert(!state->truncated);

    switch (state->status) {
        case TSS_INMEM:
            readptr->eof_reached = false;
            readptr->current = 0;
            break;
        case TSS_WRITEFILE:
            readptr->eof_reached = false;
            readptr->file = 0;
            readptr->offset = 0L;
            break;
        case TSS_READFILE:
            readptr->eof_reached = false;
            if (BufFileSeek(state->myfile, 0, 0L, SEEK_SET) != 0)
                ereport(ERROR,
                        (errcode_for_file_access(),
                                errmsg("could not seek in tuplestore temporary file")));
            break;
        default:
            elog(ERROR, "invalid tuplestore state");
            break;
    }
}

/*
 * tuplestore_copy_read_pointer - copy a read pointer's state to another
 */
void
tuplestore_copy_read_pointer(Tuplestorestate *state,
                             int srcptr, int destptr) {
    TSReadPointer *sptr = &state->readptrs[srcptr];
    TSReadPointer *dptr = &state->readptrs[destptr];

    Assert(srcptr >= 0 && srcptr < state->readptrcount);
    Assert(destptr >= 0 && destptr < state->readptrcount);

    /* Assigning to self is a no-op */
    if (srcptr == destptr)
        return;

    if (dptr->eflags != sptr->eflags) {
        /* Possible change of overall eflags, so copy and then recompute */
        int eflags;
        int i;

        *dptr = *sptr;
        eflags = state->readptrs[0].eflags;
        for (i = 1; i < state->readptrcount; i++)
            eflags |= state->readptrs[i].eflags;
        state->eflags = eflags;
    } else
        *dptr = *sptr;

    switch (state->status) {
        case TSS_INMEM:
        case TSS_WRITEFILE:
            /* no work */
            break;
        case TSS_READFILE:

            /*
             * This case is a bit tricky since the active read pointer's
             * position corresponds to the seek point, not what is in its
             * variables.  Assigning to the active requires a seek, and
             * assigning from the active requires a tell, except when
             * eof_reached.
             */
            if (destptr == state->activeptr) {
                if (dptr->eof_reached) {
                    if (BufFileSeek(state->myfile,
                                    state->writepos_file,
                                    state->writepos_offset,
                                    SEEK_SET) != 0)
                        ereport(ERROR,
                                (errcode_for_file_access(),
                                        errmsg("could not seek in tuplestore temporary file")));
                } else {
                    if (BufFileSeek(state->myfile,
                                    dptr->file, dptr->offset,
                                    SEEK_SET) != 0)
                        ereport(ERROR,
                                (errcode_for_file_access(),
                                        errmsg("could not seek in tuplestore temporary file")));
                }
            } else if (srcptr == state->activeptr) {
                if (!dptr->eof_reached)
                    BufFileTell(state->myfile,
                                &dptr->file,
                                &dptr->offset);
            }
            break;
        default:
            elog(ERROR, "invalid tuplestore state");
            break;
    }
}

/*
 * tuplestore_trim	- remove all no-longer-needed tuples
 *
 * Calling this function authorizes the tuplestore to delete all tuples
 * before the oldest read pointer, if no read pointer is marked as requiring
 * REWIND capability.
 *
 * Note: this is obviously safe if no pointer has BACKWARD capability either.
 * If a pointer is marked as BACKWARD but not REWIND capable, it means that
 * the pointer can be moved backward but not before the oldest other read
 * pointer.
 */
void
tuplestore_trim(Tuplestorestate *state) {
    int oldest;
    int nremove;
    int i;

    /*
     * Truncation is disallowed if any read pointer requires rewind
     * capability.
     */
    if (state->eflags & EXEC_FLAG_REWIND)
        return;

    /*
     * We don't bother trimming temp files since it usually would mean more
     * work than just letting them sit in kernel buffers until they age out.
     */
    if (state->status != TSS_INMEM)
        return;

    /* Find the oldest read pointer */
    oldest = state->memtupcount;
    for (i = 0; i < state->readptrcount; i++) {
        if (!state->readptrs[i].eof_reached)
            oldest = Min(oldest, state->readptrs[i].current);
    }

    /*
     * Note: you might think we could remove all the tuples before the oldest
     * "current", since that one is the next to be returned.  However, since
     * tuplestore_gettuple returns a direct pointer to our internal copy of
     * the tuple, it's likely that the caller has still got the tuple just
     * before "current" referenced in a slot. So we keep one extra tuple
     * before the oldest "current".  (Strictly speaking, we could require such
     * callers to use the "copy" flag to tuplestore_gettupleslot, but for
     * efficiency we allow this one case to not use "copy".)
     */
    nremove = oldest - 1;
    if (nremove <= 0)
        return;                    /* nothing to do */

    Assert(nremove >= state->memtupdeleted);
    Assert(nremove <= state->memtupcount);

    /* Release no-longer-needed tuples */
    for (i = state->memtupdeleted; i < nremove; i++) {
        FREEMEM(state, GetMemoryChunkSpace(state->memtuples[i]));
        pfree(state->memtuples[i]);
        state->memtuples[i] = NULL;
    }
    state->memtupdeleted = nremove;

    /* mark tuplestore as truncated (used for Assert crosschecks only) */
    state->truncated = true;

    /*
     * If nremove is less than 1/8th memtupcount, just stop here, leaving the
     * "deleted" slots as NULL.  This prevents us from expending O(N^2) time
     * repeatedly memmove-ing a large pointer array.  The worst case space
     * wastage is pretty small, since it's just pointers and not whole tuples.
     */
    if (nremove < state->memtupcount / 8)
        return;

    /*
     * Slide the array down and readjust pointers.
     *
     * In mergejoin's current usage, it's demonstrable that there will always
     * be exactly one non-removed tuple; so optimize that case.
     */
    if (nremove + 1 == state->memtupcount)
        state->memtuples[0] = state->memtuples[nremove];
    else
        memmove(state->memtuples, state->memtuples + nremove,
                (state->memtupcount - nremove) * sizeof(void *));

    state->memtupdeleted = 0;
    state->memtupcount -= nremove;
    for (i = 0; i < state->readptrcount; i++) {
        if (!state->readptrs[i].eof_reached)
            state->readptrs[i].current -= nremove;
    }
}

/*
 * tuplestore_in_memory
 *
 * Returns true if the tuplestore has not spilled to disk.
 *
 * XXX exposing this is a violation of modularity ... should get rid of it.
 */
bool
tuplestore_in_memory(Tuplestorestate *state) {
    return (state->status == TSS_INMEM);
}

// 读取了tuplestorestate对应临时文件中的下个tuple相关数据的打头4个字节长度信息
// 4个字节+tuple数据+4个字节+tuple数据
static unsigned int getlen(Tuplestorestate *tuplestorestate, bool eofOK) {
    unsigned int len;
    size_t nbytes;

    nbytes = BufFileRead(tuplestorestate->myfile, (void *) &len, sizeof(len));
    if (nbytes == sizeof(len))
        return len;

    if (nbytes != 0 || !eofOK) {
        ereport(ERROR,
                (errcode_for_file_access(),
                        errmsg("could not read from tuplestore temporary file: read only %zu of %zu bytes",
                               nbytes, sizeof(len))));
    }

    return 0;
}

/*
 * Routines specialized for HeapTuple case
 *
 * The stored form is actually a MinimalTuple, but for largely historical
 * reasons we allow COPYTUP to work from a HeapTuple.
 *
 * Since MinimalTuple already has length in its first word, we don't need
 * to write that separately.
 */
static void *copytup_heap(Tuplestorestate *tuplestorestate, void *tuple) {
    MinimalTuple minimalTuple = minimal_tuple_from_heap_tuple((HeapTuple) tuple);
    USEMEM(tuplestorestate, GetMemoryChunkSpace(minimalTuple));
    return (void *) minimalTuple;
}

static void writetup_heap(Tuplestorestate *tuplestorestate, void *tuple) {
    MinimalTuple minimalTuple = (MinimalTuple) tuple;

    /* the part of the MinimalTuple we'll write: */
    char *tupleBody = (char *) minimalTuple + MINIMAL_TUPLE_DATA_OFFSET;
    unsigned int tupleBodyLen = minimalTuple->t_len - MINIMAL_TUPLE_DATA_OFFSET;

    /* total on-disk footprint: */
    unsigned int tupleLen = tupleBodyLen + sizeof(int);

    BufFileWrite(tuplestorestate->myfile, (void *) &tupleLen, sizeof(tupleLen));
    BufFileWrite(tuplestorestate->myfile, (void *) tupleBody, tupleBodyLen);

    /* need trailing length word? */
    if (tuplestorestate->backward) {
        BufFileWrite(tuplestorestate->myfile, (void *) &tupleLen, sizeof(tupleLen));
    }

    FREEMEM(tuplestorestate, GetMemoryChunkSpace(minimalTuple));
    heap_free_minimal_tuple(minimalTuple);
}

static void *readtup_heap(Tuplestorestate *tuplestorestate, unsigned int len) {
    unsigned int tupleBodyLen = len - sizeof(int);
    unsigned int tupleLen = tupleBodyLen + MINIMAL_TUPLE_DATA_OFFSET;

    MinimalTuple minimalTuple = (MinimalTuple) palloc(tupleLen);
    char *tupleBody = (char *) minimalTuple + MINIMAL_TUPLE_DATA_OFFSET;

    USEMEM(tuplestorestate, GetMemoryChunkSpace(minimalTuple));

    // read in the tuple proper
    minimalTuple->t_len = tupleLen;

    size_t byteLenRead = BufFileRead(tuplestorestate->myfile, (void *) tupleBody, tupleBodyLen);
    if (byteLenRead != (size_t) tupleBodyLen) {
        ereport(ERROR,
                (errcode_for_file_access(),
                        errmsg("could not read from tuplestore temporary file: read only %zu of %zu bytes",
                               byteLenRead, (size_t) tupleBodyLen)));
    }

    // need trailing length word? */
    if (tuplestorestate->backward) {
        byteLenRead = BufFileRead(tuplestorestate->myfile, (void *) &tupleLen, sizeof(tupleLen));
        if (byteLenRead != sizeof(tupleLen))
            ereport(ERROR,
                    (errcode_for_file_access(),
                            errmsg("could not read from tuplestore temporary file: read only %zu of %zu bytes",
                                   byteLenRead, sizeof(tupleLen))));
    }

    return (void *) minimalTuple;
}
