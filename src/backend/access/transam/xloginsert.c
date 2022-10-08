/*-------------------------------------------------------------------------
 *
 * xloginsert.c
 *		Functions for constructing WAL records
 *
 * Constructing a WAL record begins with a call to XLogBeginInsert,
 * followed by a number of XLogRegister* calls. The registered data is
 * collected in private working memory, and finally assembled into a chain
 * of XLogRecData structs by a call to XLogRecordAssemble(). See
 * access/transam/README for details.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/transam/xloginsert.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "common/pg_lzcompress.h"
#include "miscadmin.h"
#include "replication/origin.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "pg_trace.h"

/* Buffer size required to store a compressed version of backup block image */
#define PGLZ_MAX_BLCKSZ PGLZ_MAX_OUTPUT(BLCKSZ)

/*
 * For each block reference registered with XLogRegisterBuffer, we fill in
 * a registered_buffer struct.
 */
typedef struct {
    bool in_use;            /* is this slot in use? */
    uint8 flags;            /* REGBUF_* flags */
    RelFileNode rnode;            /* identifies the relation and block */
    ForkNumber forkno;
    BlockNumber block;
    Page page;            /* page content */
    uint32 rdata_len;        /* total length of data in rdata chain */
    XLogRecData *rdata_head;    /* head of the chain of data registered with
								 * this block */
    XLogRecData *rdata_tail;    /* last entry in the chain, or &rdata_head if
								 * empty */

    XLogRecData bkp_rdatas[2];    /* temporary rdatas used to hold references to
								 * backup block data in XLogRecordAssemble() */

    /* buffer to store a compressed version of backup block image */
    char compressed_page[PGLZ_MAX_BLCKSZ];
} registered_buffer;

static registered_buffer *registered_buffers;
static int max_registered_buffers; /* allocated size */
static int max_registered_block_id = 0;    /* highest block_id + 1 currently registered */

/*
 * A chain of XLogRecDatas to hold the "main data" of a WAL record, registered
 * with XLogRegisterData(...).
 */
static XLogRecData *mainrdata_head;
static XLogRecData *mainrdata_last = (XLogRecData *) &mainrdata_head;
static uint32 mainrdata_len;    /* total # of bytes in chain */

/* flags for the in-progress insertion */
static uint8 curinsert_flags = 0;

/*
 * These are used to hold the record header while constructing a record.
 * 'hdr_scratch' is not a plain variable, but is palloc'd at initialization,
 * because we want it to be MAXALIGNed and padding bytes zeroed.
 *
 * For simplicity, it's allocated large enough to hold the headers for any
 * WAL record.
 */
static XLogRecData hdr_rdt;
static char *hdr_scratch = NULL;

#define SizeOfXlogOrigin    (sizeof(RepOriginId) + sizeof(char))

#define HEADER_SCRATCH_SIZE \
    (SizeOfXLogRecord + \
     MaxSizeOfXLogRecordBlockHeader * (XLR_MAX_BLOCK_ID + 1) + \
     SizeOfXLogRecordDataHeaderLong + SizeOfXlogOrigin)

// array of XLogRecData structs, to hold registered data.
static XLogRecData *rdatas;
static int num_rdatas;            /* entries currently used */
static int max_rdatas;            /* allocated 个数 */

static bool begininsert_called = false;

/* Memory context to hold the registered buffer and data references. */
static MemoryContext xloginsert_cxt;

static XLogRecData *XLogRecordAssemble(RmgrId rmgrId,
                                       uint8 info,
                                       XLogRecPtr redoXlogRecPtr,
                                       bool doPageWrites,
                                       XLogRecPtr *fpw_lsn);

static bool XLogCompressBackupBlock(char *page, uint16 hole_offset,
                                    uint16 hole_length, char *dest, uint16 *dlen);

/*
 * Begin constructing a WAL record. This must be called before the
 * XLogRegister* functions and XLogInsert().
 */
void XLogBeginInsert(void) {
    Assert(max_registered_block_id == 0);
    Assert(mainrdata_last == (XLogRecData *) &mainrdata_head);
    Assert(mainrdata_len == 0);

    // cross-check on whether we should be here or not */
    if (!XLogInsertAllowed()) {
        elog(ERROR, "cannot make new WAL entries during recovery");
    }

    if (begininsert_called) {
        elog(ERROR, "XLogBeginInsert was already called");
    }

    begininsert_called = true;
}

/*
 * Ensure that there are enough buffer and data slots in the working area,
 * for subsequent XLogRegisterBuffer, XLogRegisterData and XLogRegisterBufData
 * calls.
 *
 * There is always space for a small number of buffers and data chunks, enough
 * for most record types. This function is for the exceptional cases that need
 * more.
 */
void
XLogEnsureRecordSpace(int max_block_id, int ndatas) {
    int nbuffers;

    /*
     * This must be called before entering a critical section, because
     * allocating memory inside a critical section can fail. repalloc() will
     * check the same, but better to check it here too so that we fail
     * consistently even if the arrays happen to be large enough already.
     */
    Assert(CritSectionCount == 0);

    /* the minimum values can't be decreased */
    if (max_block_id < XLR_NORMAL_MAX_BLOCK_ID)
        max_block_id = XLR_NORMAL_MAX_BLOCK_ID;
    if (ndatas < XLR_NORMAL_RDATAS)
        ndatas = XLR_NORMAL_RDATAS;

    if (max_block_id > XLR_MAX_BLOCK_ID)
        elog(ERROR, "maximum number of WAL record block references exceeded");
    nbuffers = max_block_id + 1;

    if (nbuffers > max_registered_buffers) {
        registered_buffers = (registered_buffer *)
                repalloc(registered_buffers, sizeof(registered_buffer) * nbuffers);

        /*
         * At least the padding bytes in the structs must be zeroed, because
         * they are included in WAL data, but initialize it all for tidiness.
         */
        MemSet(&registered_buffers[max_registered_buffers], 0,
               (nbuffers - max_registered_buffers) * sizeof(registered_buffer));
        max_registered_buffers = nbuffers;
    }

    if (ndatas > max_rdatas) {
        rdatas = (XLogRecData *) repalloc(rdatas, sizeof(XLogRecData) * ndatas);
        max_rdatas = ndatas;
    }
}

// Reset WAL record construction buffers 干完了之后的清理
void XLogResetInsertion(void) {
    for (int i = 0; i < max_registered_block_id; i++) {
        registered_buffers[i].in_use = false;
    }

    num_rdatas = 0;
    max_registered_block_id = 0;
    mainrdata_len = 0;
    mainrdata_last = (XLogRecData *) &mainrdata_head;
    curinsert_flags = 0;
    begininsert_called = false;
}

/*
 * 使用 registered_buffers
 *
 * Register a reference to a buffer with the WAL record being constructed.
 * This must be called for every page that the WAL-logged operation modifies.
 */
void XLogRegisterBuffer(uint8 block_id, Buffer buffer, uint8 flags) {
    /* NO_IMAGE doesn't make sense with FORCE_IMAGE */
    Assert(!((flags & REGBUF_FORCE_IMAGE) && (flags & (REGBUF_NO_IMAGE))));
    Assert(begininsert_called);

    if (block_id >= max_registered_block_id) {
        if (block_id >= max_registered_buffers)
            elog(ERROR, "too many registered buffers");
        max_registered_block_id = block_id + 1;
    }

    registered_buffer *registeredBuffer = &registered_buffers[block_id];

    BufferGetTag(buffer,
                 &registeredBuffer->rnode,
                 &registeredBuffer->forkno,
                 &registeredBuffer->block);

    registeredBuffer->page = BufferGetPage(buffer);
    registeredBuffer->flags = flags;
    registeredBuffer->rdata_tail = (XLogRecData *) &registeredBuffer->rdata_head;
    registeredBuffer->rdata_len = 0;

    // check that this page hasn't already been registered with some other block_id
#ifdef USE_ASSERT_CHECKING
    {
        for (int i = 0; i < max_registered_block_id; i++) {
            registered_buffer *regbuf_old = &registered_buffers[i];

            if (i == block_id || !regbuf_old->in_use) {
                continue;
            }

            Assert(!RelFileNodeEquals(regbuf_old->rnode, registeredBuffer->rnode) ||
                   regbuf_old->forkno != registeredBuffer->forkno ||
                   regbuf_old->block != registeredBuffer->block);
        }
    }
#endif

    registeredBuffer->in_use = true;
}

/*
 * Like XLogRegisterBuffer, but for registering a block that's not in the
 * shared buffer pool (i.e. when you don't have a Buffer for it).
 */
void
XLogRegisterBlock(uint8 block_id, RelFileNode *rnode, ForkNumber forknum,
                  BlockNumber blknum, Page page, uint8 flags) {
    registered_buffer *regbuf;

    /* This is currently only used to WAL-log a full-page image of a page */
    Assert(flags & REGBUF_FORCE_IMAGE);
    Assert(begininsert_called);

    if (block_id >= max_registered_block_id)
        max_registered_block_id = block_id + 1;

    if (block_id >= max_registered_buffers)
        elog(ERROR, "too many registered buffers");

    regbuf = &registered_buffers[block_id];

    regbuf->rnode = *rnode;
    regbuf->forkno = forknum;
    regbuf->block = blknum;
    regbuf->page = page;
    regbuf->flags = flags;
    regbuf->rdata_tail = (XLogRecData *) &regbuf->rdata_head;
    regbuf->rdata_len = 0;

    /*
     * Check that this page hasn't already been registered with some other
     * block_id.
     */
#ifdef USE_ASSERT_CHECKING
    {
        int i;

        for (i = 0; i < max_registered_block_id; i++) {
            registered_buffer *regbuf_old = &registered_buffers[i];

            if (i == block_id || !regbuf_old->in_use)
                continue;

            Assert(!RelFileNodeEquals(regbuf_old->rnode, regbuf->rnode) ||
                   regbuf_old->forkno != regbuf->forkno ||
                   regbuf_old->block != regbuf->block);
        }
    }
#endif

    regbuf->in_use = true;
}

/*
 * 使用 rdatas
 * Add data to the WAL record that's being constructed.
 * The data is appended to the "main chunk", available at replay with XLogRecGetData().
 */
void XLogRegisterData(char *data, int len) {
    Assert(begininsert_called);

    if (num_rdatas >= max_rdatas) {
        elog(ERROR, "too much WAL data");
    }

    XLogRecData *xLogRecData = &rdatas[num_rdatas++];

    xLogRecData->data = data;
    xLogRecData->len = len;

    // we use the mainrdata_last pointer to track the end of the chain, so no need to clear 'next' here.
    mainrdata_last->next = xLogRecData;
    mainrdata_last = xLogRecData;

    mainrdata_len += len;
}

/*
 * Add buffer-specific data to the WAL record that's being constructed.
 *
 * Block_id must reference a block previously registered with
 * XLogRegisterBuffer(). If this is called more than once for the same
 * block_id, the data is appended.
 *
 * The maximum amount of data that can be registered per block is 65535
 * bytes. That should be plenty; if you need more than BLCKSZ bytes to
 * reconstruct the changes to the page, you might as well just log a full
 * copy of it. (the "main data" that's not associated with a block is not
 * limited)
 */
void XLogRegisterBufData(uint8 block_id, char *data, int len) {
    Assert(begininsert_called);

    // find the registered buffer struct */
    registered_buffer *registeredBuffer = &registered_buffers[block_id];

    if (!registeredBuffer->in_use) {
        elog(ERROR, "no block with id %d registered with WAL insertion", block_id);
    }

    if (num_rdatas >= max_rdatas) {
        elog(ERROR, "too much WAL data");
    }

    XLogRecData *xLogRecData = &rdatas[num_rdatas++];

    xLogRecData->data = data;
    xLogRecData->len = len;

    registeredBuffer->rdata_tail->next = xLogRecData;
    registeredBuffer->rdata_tail = xLogRecData;
    registeredBuffer->rdata_len += len;
}

/*
 * Set insert status flags for the upcoming WAL record.
 *
 * The flags that can be used here are:
 * - XLOG_INCLUDE_ORIGIN, to determine if the replication origin should be
 *	 included in the record.
 * - XLOG_MARK_UNIMPORTANT, to signal that the record is not important for
 *	 durability, which allows to avoid triggering WAL archiving and other
 *	 background activity.
 */
void XLogSetRecordFlags(uint8 flags) {
    Assert(begininsert_called);
    curinsert_flags = flags;
}

/*
 * Insert an XLOG record having the specified RMID and info bytes, with the
 * body of the record being the data and buffer references registered earlier
 * with XLogRegister* calls.
 *
 * Returns XLOG pointer to end of record (beginning of next record).
 * This can be used as LSN for data pages affected by the logged action.
 * (LSN is the XLOG point up to which the XLOG must be flushed to disk
 * before the data page can be written out.  This implements the basic
 * WAL rule "write the log before the data".)
 */
XLogRecPtr XLogInsert(RmgrId rmgrId, uint8 info) {

    /* XLogBeginInsert() must have been called. */
    if (!begininsert_called) {
        elog(ERROR, "XLogBeginInsert was not called");
    }

    /*
     * The caller can set rmgr bits, XLR_SPECIAL_REL_UPDATE and
     * XLR_CHECK_CONSISTENCY; the rest are reserved for use by me.
     */
    if ((info & ~(XLR_RMGR_INFO_MASK | XLR_SPECIAL_REL_UPDATE | XLR_CHECK_CONSISTENCY)) != 0) {
        elog(PANIC, "invalid xlog info mask %02X", info);
    }

    TRACE_POSTGRESQL_WAL_INSERT(rmgrId, info);

    // in bootstrap mode, we don't actually log anything but XLOG resources; return a phony record pointer.
    if (IsBootstrapProcessingMode() && rmgrId != RM_XLOG_ID) {
        XLogResetInsertion();
        return SizeOfXLogLongPHD; /* start of 1st chkpt record */
    }

    XLogRecPtr endPos;
    do {
        // Get values needed to decide whether to do full-page writes. Since
        // we don't yet have an insertion lock, these could change under us,
        // but XLogInsertRecord will recheck them once it has a lock.
        bool doPageWrites;
        XLogRecPtr redoXlogRecPtr;

        GetFullPageWriteInfo(&redoXlogRecPtr, &doPageWrites);

        XLogRecPtr fpw_lsn;
        // 得到的 xLogRecData 是 hdr_rdt
        XLogRecData *xLogRecData = XLogRecordAssemble(rmgrId,
                                                      info,
                                                      redoXlogRecPtr,
                                                      doPageWrites,
                                                      &fpw_lsn);

        endPos = XLogInsertRecord(xLogRecData, fpw_lsn, curinsert_flags);
    } while (endPos == InvalidXLogRecPtr);

    XLogResetInsertion();

    return endPos;
}

/*
 * 相应的信息通过memcpy以网络通信协议的格式记录到 起始地址是 hdr_scratch 这片内存上
 * xLogRecord+xLogRecordBlockHeader+[xLogRecordBlockImageHeader]+[xLogRecordBlockCompressHeader]+[rnode]+blockNumber
 *
 * assemble a WAL record from the registered data and buffers into an
 * XLogRecData chain, ready for insertion with XLogInsertRecord().
 *
 * The record header fields are filled in, except for the xl_prev field. The
 * calculated CRC does not include the record header yet.
 *
 * If there are any registered buffers, and a full-page image was not taken
 * of all of them, *fpw_lsn is set to the lowest LSN among such pages. This
 * signals that the assembled record is only good for insertion on the
 * assumption that the redoXlogRecPtr and doPageWrites values were up-to-date.
 */
static XLogRecData *XLogRecordAssemble(RmgrId rmgrId,
                                       uint8 info,
                                       XLogRecPtr redoXlogRecPtr,
                                       bool doPageWrites,
                                       XLogRecPtr *fpw_lsn) {
    uint32 totalLen = 0;
    registered_buffer *prev_regbuf = NULL;

    char *scratchCurPos = hdr_scratch;

    /*
     * this function can be called multiple times for the same record.
     * All the modifications we do to the rdata chains below must handle that.
     */

    // The record begins with the fixed-size header
    XLogRecord *xLogRecordInScratch = (XLogRecord *) scratchCurPos;
    scratchCurPos += SizeOfXLogRecord;

    hdr_rdt.next = NULL;
    XLogRecData *xLogRecDataCurrent_hdr_rdt_series = &hdr_rdt;
    hdr_rdt.data = hdr_scratch;

    /*
     * Enforce consistency checks for this record if user is looking for it.
     * Do this before at the beginning of this routine to give the possibility
     * for callers of XLogInsert() to pass XLR_CHECK_CONSISTENCY directly for a record.
     */
    if (wal_consistency_checking[rmgrId]) {
        info |= XLR_CHECK_CONSISTENCY;
    }

    /*
     * Make a rdata chain containing all the data portions of all block
     * references. This includes the data for full-page images. Also append
     * the headers for the block references in the scratchCurPos buffer.
     */
    *fpw_lsn = InvalidXLogRecPtr;
    for (int blockId = 0; blockId < max_registered_block_id; blockId++) {
        registered_buffer *registeredBuffer = &registered_buffers[blockId];

        if (!registeredBuffer->in_use) {
            continue;
        }


        XLogRecordBlockImageHeader xLogRecordBlockImageHeader;
        XLogRecordBlockCompressHeader xLogRecordBlockCompressHeader = {0};

        // determine if this block need backed up
        bool needsBackup;
        if (registeredBuffer->flags & REGBUF_FORCE_IMAGE) {
            needsBackup = true;
        } else if (registeredBuffer->flags & REGBUF_NO_IMAGE) {
            needsBackup = false;
        } else if (!doPageWrites) {
            needsBackup = false;
        } else {
            // We assume page LSN is first data on *every* page that can be
            // passed to XLogInsert, whether it has the standard page layout or not.
            XLogRecPtr page_lsn = PageGetLSN(registeredBuffer->page);

            needsBackup = (page_lsn <= redoXlogRecPtr);
            if (!needsBackup) {
                if (*fpw_lsn == InvalidXLogRecPtr || page_lsn < *fpw_lsn) {
                    *fpw_lsn = page_lsn;
                }
            }
        }

        // determine if the buffer data needs to included
        bool needsData;
        if (registeredBuffer->rdata_len == 0) {
            needsData = false;
        } else if ((registeredBuffer->flags & REGBUF_KEEP_DATA) != 0) {
            needsData = true;
        } else {
            needsData = !needsBackup;
        }

        XLogRecordBlockHeader xLogRecordBlockHeader;
        xLogRecordBlockHeader.id = blockId;
        xLogRecordBlockHeader.fork_flags = registeredBuffer->forkno;
        xLogRecordBlockHeader.data_length = 0;

        if ((registeredBuffer->flags & REGBUF_WILL_INIT) == REGBUF_WILL_INIT) {
            xLogRecordBlockHeader.fork_flags |= BKPBLOCK_WILL_INIT;
        }

        bool isCompressed = false;

        /*
         * If needsBackup is true or WAL checking is enabled for current
         * resource manager, log a full-page write for the current block.
         */
        bool includeImage = needsBackup || (info & XLR_CHECK_CONSISTENCY) != 0;
        if (includeImage) {
            Page page = registeredBuffer->page;
            uint16 compressedLen = 0;

            // The page needs to be backed up, so calculate its hole length and offset.
            if (registeredBuffer->flags & REGBUF_STANDARD) {
                /* Assume we can omit data between pd_lower and pd_upper */
                uint16 lower = ((PageHeader) page)->pd_lower;
                uint16 upper = ((PageHeader) page)->pd_upper;

                if (lower >= SizeOfPageHeaderData &&
                    upper > lower &&
                    upper <= BLCKSZ) {
                    xLogRecordBlockImageHeader.hole_offset = lower;
                    xLogRecordBlockCompressHeader.hole_length = upper - lower; // 对应了page的空闲的space
                } else {
                    /* No "hole" to remove */
                    xLogRecordBlockImageHeader.hole_offset = 0;
                    xLogRecordBlockCompressHeader.hole_length = 0;
                }
            } else {
                // Not a standard page header, don't try to eliminate "hole" */
                xLogRecordBlockImageHeader.hole_offset = 0;
                xLogRecordBlockCompressHeader.hole_length = 0;
            }

            // Try to compress a block image if wal_compression is enabled
            if (wal_compression) {
                isCompressed = XLogCompressBackupBlock(page, xLogRecordBlockImageHeader.hole_offset,
                                                       xLogRecordBlockCompressHeader.hole_length,
                                                       registeredBuffer->compressed_page,
                                                       &compressedLen);
            }

            // fill in the remaining fields in the XLogRecordBlockHeader struct
            xLogRecordBlockHeader.fork_flags |= BKPBLOCK_HAS_IMAGE;

            // construct XLogRecData entries for the page content.
            xLogRecDataCurrent_hdr_rdt_series->next = &registeredBuffer->bkp_rdatas[0];
            xLogRecDataCurrent_hdr_rdt_series = xLogRecDataCurrent_hdr_rdt_series->next;

            xLogRecordBlockImageHeader.bimg_info = (xLogRecordBlockCompressHeader.hole_length == 0) ? 0
                                                                                                    : BKPIMAGE_HAS_HOLE;

            /*
             * If WAL consistency checking is enabled for the resource manager
             * of this WAL record, a full-page image is included in the record
             * for the block modified. During redo, the full-page is replayed
             * only if BKPIMAGE_APPLY is set.
             */
            if (needsBackup)
                xLogRecordBlockImageHeader.bimg_info |= BKPIMAGE_APPLY;

            if (isCompressed) {
                xLogRecordBlockImageHeader.length = compressedLen;
                xLogRecordBlockImageHeader.bimg_info |= BKPIMAGE_IS_COMPRESSED;

                xLogRecDataCurrent_hdr_rdt_series->data = registeredBuffer->compressed_page;
                xLogRecDataCurrent_hdr_rdt_series->len = compressedLen;
            } else {
                xLogRecordBlockImageHeader.length = BLCKSZ - xLogRecordBlockCompressHeader.hole_length;// 挖掉当中的hole

                if (xLogRecordBlockCompressHeader.hole_length == 0) {
                    xLogRecDataCurrent_hdr_rdt_series->data = page;
                    xLogRecDataCurrent_hdr_rdt_series->len = BLCKSZ;
                } else {
                    // must skip the hole  分为了2个部分:page起点到lower ,upper到page末尾
                    xLogRecDataCurrent_hdr_rdt_series->data = page;
                    xLogRecDataCurrent_hdr_rdt_series->len = xLogRecordBlockImageHeader.hole_offset;

                    xLogRecDataCurrent_hdr_rdt_series->next = &registeredBuffer->bkp_rdatas[1];
                    xLogRecDataCurrent_hdr_rdt_series = xLogRecDataCurrent_hdr_rdt_series->next;

                    xLogRecDataCurrent_hdr_rdt_series->data =
                            page + (xLogRecordBlockImageHeader.hole_offset + xLogRecordBlockCompressHeader.hole_length);
                    xLogRecDataCurrent_hdr_rdt_series->len =
                            BLCKSZ -
                            (xLogRecordBlockImageHeader.hole_offset + xLogRecordBlockCompressHeader.hole_length);
                }
            }

            totalLen += xLogRecordBlockImageHeader.length;
        }

        if (needsData) {
            // Link the caller-supplied rdata chain for this buffer to the overall list.
            xLogRecordBlockHeader.fork_flags |= BKPBLOCK_HAS_DATA;
            xLogRecordBlockHeader.data_length = registeredBuffer->rdata_len;
            totalLen += registeredBuffer->rdata_len;

            xLogRecDataCurrent_hdr_rdt_series->next = registeredBuffer->rdata_head;
            xLogRecDataCurrent_hdr_rdt_series = registeredBuffer->rdata_tail;
        }

        bool sameRelation;
        if (prev_regbuf && RelFileNodeEquals(registeredBuffer->rnode, prev_regbuf->rnode)) {
            sameRelation = true;
            xLogRecordBlockHeader.fork_flags |= BKPBLOCK_SAME_REL;
        } else {
            sameRelation = false;
        }

        prev_regbuf = registeredBuffer;

        // ok, copy the header to the hdr_scratch buffer */
        memcpy(scratchCurPos, &xLogRecordBlockHeader, SizeOfXLogRecordBlockHeader);
        scratchCurPos += SizeOfXLogRecordBlockHeader;

        if (includeImage) {
            memcpy(scratchCurPos, &xLogRecordBlockImageHeader, SizeOfXLogRecordBlockImageHeader);
            scratchCurPos += SizeOfXLogRecordBlockImageHeader;

            if (xLogRecordBlockCompressHeader.hole_length != 0 && isCompressed) {
                memcpy(scratchCurPos, &xLogRecordBlockCompressHeader, SizeOfXLogRecordBlockCompressHeader);
                scratchCurPos += SizeOfXLogRecordBlockCompressHeader;
            }
        }

        if (!sameRelation) {
            memcpy(scratchCurPos, &registeredBuffer->rnode, sizeof(RelFileNode));
            scratchCurPos += sizeof(RelFileNode);
        }

        memcpy(scratchCurPos, &registeredBuffer->block, sizeof(BlockNumber));
        scratchCurPos += sizeof(BlockNumber);
    }

    // followed by the record's origin
    if ((curinsert_flags & XLOG_INCLUDE_ORIGIN) && replorigin_session_origin != InvalidRepOriginId) {
        *(scratchCurPos++) = (char) XLR_BLOCK_ID_ORIGIN;
        memcpy(scratchCurPos, &replorigin_session_origin, sizeof(replorigin_session_origin));
        scratchCurPos += sizeof(replorigin_session_origin);
    }

    // followed by main data
    if (mainrdata_len > 0) {
        if (mainrdata_len > 255) {
            *(scratchCurPos++) = (char) XLR_BLOCK_ID_DATA_LONG;
            memcpy(scratchCurPos, &mainrdata_len, sizeof(uint32));
            scratchCurPos += sizeof(uint32);
        } else {
            *(scratchCurPos++) = (char) XLR_BLOCK_ID_DATA_SHORT;
            *(scratchCurPos++) = (uint8) mainrdata_len;
        }
        xLogRecDataCurrent_hdr_rdt_series->next = mainrdata_head; // 构成了环
        xLogRecDataCurrent_hdr_rdt_series = mainrdata_last;
        totalLen += mainrdata_len;
    }
    xLogRecDataCurrent_hdr_rdt_series->next = NULL;

    hdr_rdt.len = (scratchCurPos - hdr_scratch);
    totalLen += hdr_rdt.len;

    /*
     * Calculate CRC of the data
     *
     * Note that the record header isn't added into the CRC initially since we
     * don't know the prev-link yet.  Thus, the CRC will represent the CRC of
     * the whole record in the order: rdata, then backup blocks, then record header.
     */
    pg_crc32c rdata_crc;
    INIT_CRC32C(rdata_crc);
    COMP_CRC32C(rdata_crc, hdr_scratch + SizeOfXLogRecord, hdr_rdt.len - SizeOfXLogRecord);
    for (XLogRecData *xLogRecData = hdr_rdt.next; xLogRecData != NULL; xLogRecData = xLogRecData->next) {
        COMP_CRC32C(rdata_crc, xLogRecData->data, xLogRecData->len);
    }

    /*
     * Fill in the fields in the record header. Prev-link is filled in later,
     * once we know where in the WAL the record will be inserted. The CRC does
     * not include the record header yet.
     */
    xLogRecordInScratch->xl_xid = GetCurrentTransactionIdIfAny();
    xLogRecordInScratch->xl_tot_len = totalLen;
    xLogRecordInScratch->xl_info = info;
    xLogRecordInScratch->xl_rmid = rmgrId;
    xLogRecordInScratch->xl_prev = InvalidXLogRecPtr;
    xLogRecordInScratch->xl_crc = rdata_crc;

    return &hdr_rdt;
}

/*
 * Create a compressed version of a backup block image.
 *
 * Returns false if compression fails (i.e., compressed result is actually
 * bigger than original). Otherwise, returns true and sets 'dlen' to
 * the length of compressed block image.
 */
static bool XLogCompressBackupBlock(char *page, uint16 hole_offset, uint16 hole_length,
                                    char *dest, uint16 *dlen) {
    int32 orig_len = BLCKSZ - hole_length;
    int32 len;
    int32 extra_bytes = 0;
    char *source;
    PGAlignedBlock tmp;

    if (hole_length != 0) {
        /* must skip the hole */
        source = tmp.data;
        memcpy(source, page, hole_offset);
        memcpy(source + hole_offset,
               page + (hole_offset + hole_length),
               BLCKSZ - (hole_length + hole_offset));

        /*
         * Extra data needs to be stored in WAL record for the compressed
         * version of block image if the hole exists.
         */
        extra_bytes = SizeOfXLogRecordBlockCompressHeader;
    } else
        source = page;

    /*
     * We recheck the actual size even if pglz_compress() reports success and
     * see if the number of bytes saved by compression is larger than the
     * length of extra data needed for the compressed version of block image.
     */
    len = pglz_compress(source, orig_len, dest, PGLZ_strategy_default);
    if (len >= 0 &&
        len + extra_bytes < orig_len) {
        *dlen = (uint16) len;    /* successful compression */
        return true;
    }
    return false;
}

/*
 * Determine whether the buffer referenced has to be backed up.
 *
 * Since we don't yet have the insert lock, fullPageWrites and forcePageWrites
 * could change later, so the result should be used for optimization purposes
 * only.
 */
bool
XLogCheckBufferNeedsBackup(Buffer buffer) {
    XLogRecPtr RedoRecPtr;
    bool doPageWrites;
    Page page;

    GetFullPageWriteInfo(&RedoRecPtr, &doPageWrites);

    page = BufferGetPage(buffer);

    if (doPageWrites && PageGetLSN(page) <= RedoRecPtr)
        return true;            /* buffer requires backup */

    return false;                /* buffer does not need to be backed up */
}

/*
 * Write a backup block if needed when we are setting a hint. Note that
 * this may be called for a variety of page types, not just heaps.
 *
 * Callable while holding just share lock on the buffer content.
 *
 * We can't use the plain backup block mechanism since that relies on the
 * Buffer being exclusively locked. Since some modifications (setting LSN, hint
 * bits) are allowed in a sharelocked buffer that can lead to wal checksum
 * failures. So instead we copy the page and insert the copied data as normal
 * record data.
 *
 * We only need to do something if page has not yet been full page written in
 * this checkpoint round. The LSN of the inserted wal record is returned if we
 * had to write, InvalidXLogRecPtr otherwise.
 *
 * It is possible that multiple concurrent backends could attempt to write WAL
 * records. In that case, multiple copies of the same block would be recorded
 * in separate WAL records by different backends, though that is still OK from
 * a correctness perspective.
 */
XLogRecPtr
XLogSaveBufferForHint(Buffer buffer, bool buffer_std) {
    XLogRecPtr recptr = InvalidXLogRecPtr;
    XLogRecPtr lsn;
    XLogRecPtr RedoRecPtr;

    /*
     * Ensure no checkpoint can change our view of RedoRecPtr.
     */
    Assert(MyPgXact->delayChkpt);

    /*
     * Update RedoRecPtr so that we can make the right decision
     */
    RedoRecPtr = GetRedoRecPtr();

    /*
     * We assume page LSN is first data on *every* page that can be passed to
     * XLogInsert, whether it has the standard page layout or not. Since we're
     * only holding a share-lock on the page, we must take the buffer header
     * lock when we look at the LSN.
     */
    lsn = BufferGetLSNAtomic(buffer);

    if (lsn <= RedoRecPtr) {
        int flags;
        PGAlignedBlock copied_buffer;
        char *origdata = (char *) BufferGetBlock(buffer);
        RelFileNode rnode;
        ForkNumber forkno;
        BlockNumber blkno;

        /*
         * Copy buffer so we don't have to worry about concurrent hint bit or
         * lsn updates. We assume pd_lower/upper cannot be changed without an
         * exclusive lock, so the contents bkp are not racy.
         */
        if (buffer_std) {
            /* Assume we can omit data between pd_lower and pd_upper */
            Page page = BufferGetPage(buffer);
            uint16 lower = ((PageHeader) page)->pd_lower;
            uint16 upper = ((PageHeader) page)->pd_upper;

            memcpy(copied_buffer.data, origdata, lower);
            memcpy(copied_buffer.data + upper, origdata + upper, BLCKSZ - upper);
        } else
            memcpy(copied_buffer.data, origdata, BLCKSZ);

        XLogBeginInsert();

        flags = REGBUF_FORCE_IMAGE;
        if (buffer_std)
            flags |= REGBUF_STANDARD;

        BufferGetTag(buffer, &rnode, &forkno, &blkno);
        XLogRegisterBlock(0, &rnode, forkno, blkno, copied_buffer.data, flags);

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI_FOR_HINT);
    }

    return recptr;
}

/*
 * Write a WAL record containing a full image of a page. Caller is responsible
 * for writing the page to disk after calling this routine.
 *
 * Note: If you're using this function, you should be building pages in private
 * memory and writing them directly to smgr.  If you're using buffers, call
 * log_newpage_buffer instead.
 *
 * If the page follows the standard page layout, with a PageHeader and unused
 * space between pd_lower and pd_upper, set 'page_std' to true. That allows
 * the unused space to be left out from the WAL record, making it smaller.
 */
XLogRecPtr
log_newpage(RelFileNode *rnode, ForkNumber forkNum, BlockNumber blkno,
            Page page, bool page_std) {
    int flags;
    XLogRecPtr recptr;

    flags = REGBUF_FORCE_IMAGE;
    if (page_std)
        flags |= REGBUF_STANDARD;

    XLogBeginInsert();
    XLogRegisterBlock(0, rnode, forkNum, blkno, page, flags);
    recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);

    /*
     * The page may be uninitialized. If so, we can't set the LSN because that
     * would corrupt the page.
     */
    if (!PageIsNew(page)) {
        PageSetLSN(page, recptr);
    }

    return recptr;
}

/*
 * Write a WAL record containing a full image of a page.
 *
 * Caller should initialize the buffer and mark it dirty before calling this
 * function.  This function will set the page LSN.
 *
 * If the page follows the standard page layout, with a PageHeader and unused
 * space between pd_lower and pd_upper, set 'page_std' to true. That allows
 * the unused space to be left out from the WAL record, making it smaller.
 */
XLogRecPtr
log_newpage_buffer(Buffer buffer, bool page_std) {
    Page page = BufferGetPage(buffer);
    RelFileNode rnode;
    ForkNumber forkNum;
    BlockNumber blkno;

    /* Shared buffers should be modified in a critical section. */
    Assert(CritSectionCount > 0);

    BufferGetTag(buffer, &rnode, &forkNum, &blkno);

    return log_newpage(&rnode, forkNum, blkno, page, page_std);
}

/*
 * WAL-log a range of blocks in a relation.
 *
 * An image of all pages with block numbers 'startblk' <= X < 'endblk' is
 * written to the WAL. If the range is large, this is done in multiple WAL
 * records.
 *
 * If all page follows the standard page layout, with a PageHeader and unused
 * space between pd_lower and pd_upper, set 'page_std' to true. That allows
 * the unused space to be left out from the WAL records, making them smaller.
 *
 * NOTE: This function acquires exclusive-locks on the pages. Typically, this
 * is used on a newly-built relation, and the caller is holding a
 * AccessExclusiveLock on it, so no other backend can be accessing it at the
 * same time. If that's not the case, you must ensure that this does not
 * cause a deadlock through some other means.
 */
void
log_newpage_range(Relation rel, ForkNumber forkNum,
                  BlockNumber startblk, BlockNumber endblk,
                  bool page_std) {
    int flags;
    BlockNumber blkno;

    flags = REGBUF_FORCE_IMAGE;
    if (page_std)
        flags |= REGBUF_STANDARD;

    /*
     * Iterate over all the pages in the range. They are collected into
     * batches of XLR_MAX_BLOCK_ID pages, and a single WAL-record is written
     * for each batch.
     */
    XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID - 1, 0);

    blkno = startblk;
    while (blkno < endblk) {
        Buffer bufpack[XLR_MAX_BLOCK_ID];
        XLogRecPtr recptr;
        int nbufs;
        int i;

        CHECK_FOR_INTERRUPTS();

        /* Collect a batch of blocks. */
        nbufs = 0;
        while (nbufs < XLR_MAX_BLOCK_ID && blkno < endblk) {
            Buffer buf = ReadBufferExtended(rel, forkNum, blkno,
                                            RBM_NORMAL, NULL);

            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

            /*
             * Completely empty pages are not WAL-logged. Writing a WAL record
             * would change the LSN, and we don't want that. We want the page
             * to stay empty.
             */
            if (!PageIsNew(BufferGetPage(buf)))
                bufpack[nbufs++] = buf;
            else
                UnlockReleaseBuffer(buf);
            blkno++;
        }

        /* Write WAL record for this batch. */
        XLogBeginInsert();

        START_CRIT_SECTION();
        for (i = 0; i < nbufs; i++) {
            XLogRegisterBuffer(i, bufpack[i], flags);
            MarkBufferDirty(bufpack[i]);
        }

        recptr = XLogInsert(RM_XLOG_ID, XLOG_FPI);

        for (i = 0; i < nbufs; i++) {
            PageSetLSN(BufferGetPage(bufpack[i]), recptr);
            UnlockReleaseBuffer(bufpack[i]);
        }
        END_CRIT_SECTION();
    }
}

/*
 * Allocate working buffers needed for WAL record construction.
 */
void
InitXLogInsert(void) {
    /* Initialize the working areas */
    if (xloginsert_cxt == NULL) {
        xloginsert_cxt = AllocSetContextCreate(TopMemoryContext,
                                               "WAL record construction",
                                               ALLOCSET_DEFAULT_SIZES);
    }

    if (registered_buffers == NULL) {
        registered_buffers = (registered_buffer *)
                MemoryContextAllocZero(xloginsert_cxt,
                                       sizeof(registered_buffer) * (XLR_NORMAL_MAX_BLOCK_ID + 1));
        max_registered_buffers = XLR_NORMAL_MAX_BLOCK_ID + 1;
    }
    if (rdatas == NULL) {
        rdatas = MemoryContextAlloc(xloginsert_cxt,
                                    sizeof(XLogRecData) * XLR_NORMAL_RDATAS);
        max_rdatas = XLR_NORMAL_RDATAS;
    }

    // Allocate a buffer to hold the header information for a WAL record.
    if (hdr_scratch == NULL) {
        hdr_scratch = MemoryContextAllocZero(xloginsert_cxt, HEADER_SCRATCH_SIZE);
    }
}
