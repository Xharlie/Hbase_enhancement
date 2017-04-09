package org.apache.hadoop.hbase.regionserver;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.ReplayHLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hbase.util.HasThreads;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.server.SyncRequestProcessor;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by xharlie on 2/20/17.
 */
public class HRegionGroupProcess {

  public int stepBarrier;

  static final Log LOG = LogFactory.getLog(HRegionGroupProcess.class);

  public WAL wal;
  public FSHLog hlog;

  // a Processor for preAppend stage.
  private PreAppendProcessor  preAppender;

  // a Processor for finish stage.
  private FinishProcessor mutateFinisher;
  private int preAppendThreadCount = 1;
  private int finishThreadCount = 2;

  public ConcurrentHashMap<byte[], Integer> indexInGroupCache;


  // TODO further handle remain process, release lock, etc.
  public String toString() {
    if (hlog != null) {
      return "RegionGroupProcess paired with" + hlog.logFilePrefix + ":"
              + hlog.logFileSuffix + "(num " + hlog.filenum + ")";
    } else {
      return "RegionGroupProcess paired with" + wal;
    }
  }

  public void close() {
    try {
      preAppender.interruptAll();
      preAppender.joinAll();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for " + preAppender.getName(0) +
              " threads to die", e);
    }

    try {
      mutateFinisher.interruptAll();
      mutateFinisher.joinAll();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for " + mutateFinisher.getName(0) +
              " threads to die", e);
    }
    LOG.info(toString() + " Closed");
  }

  public HRegionGroupProcess(WAL wal) {
    this.wal = wal;
    if(this.wal instanceof FSHLog) {
      hlog = (FSHLog)wal;
      this.stepBarrier = hlog.conf.getInt("hbase.regionserver.regiongroup.process.barrier", 1000);
      this.preAppendThreadCount = Math.max(1,
              hlog.conf.getInt("hbase.regionserver.regiongroup.process.preAppender.thread.count", 1));
      this.finishThreadCount = Math.max(2,
              hlog.conf.getInt("hbase.regionserver.regiongroup.process.finisher.thread.count", 2));
      if(finishThreadCount % 2 ==1) finishThreadCount--;
    }

    preAppender = new PreAppendProcessor(
            Thread.currentThread().getName() + "-preAppendProcessor", preAppendThreadCount);
    preAppender.startAll();

    mutateFinisher = new FinishProcessor(
            Thread.currentThread().getName() + "-finishProcessor", finishThreadCount);
    mutateFinisher.startAll();
  }

  public int getThreadIndex(HRegion hregion, int threadCount) {
    Integer num = (indexInGroupCache != null) ?
            indexInGroupCache.get(hregion.getRegionInfo().getEncodedNameAsBytes()): null;
    if (num != null) {
      return num % threadCount;
    } else {
      return Math.abs(hregion.hashCode()) % threadCount;
    }
  }

  public int getAllPreAppendQueueSize() {
    int amount = 0;
    for (ArrayBlockingQueue<ResponseToolKit> queue : preAppender.preAppendQueue) {
      amount += queue.size();
    }
    return amount;
  }

  public int getPreAppendQueueCount() {
    return preAppender.preAppendQueue.length;
  }

  public int getAllAsyncFinishQueueSize() {
    int amount = 0;
    for (ArrayBlockingQueue<WALKey> queue : mutateFinisher.asyncFinishQueue) {
      amount += queue.size();
    }
    return amount;
  }

  public int getAsyncFinishQueueCount() {
    return mutateFinisher.asyncFinishQueue.length;
  }

  public int getAllSyncFinishQueueSize() {
    int amount = 0;
    for (ArrayBlockingQueue<WALKey> queue : mutateFinisher.syncFinishQueue) {
      amount += queue.size();
    }
    return amount;
  }

  public int getSyncFinishQueueCount() {
    return mutateFinisher.syncFinishQueue.length;
  }

  public void addRequest(ResponseToolKit responseTK) throws InterruptedException {
    preAppender.preAppendQueue[getThreadIndex(responseTK.hregion,
            preAppender.preAppendQueue.length)].put(responseTK);
  }

  private class PreAppendProcessor extends AsyncProcessor {

    public ArrayBlockingQueue<ResponseToolKit>[] preAppendQueue;

    public PreAppendProcessor(String name, int workerNum) {
      super(name, workerNum);
      preAppendQueue = new ArrayBlockingQueue[workerNum];
      for (int i = 0; i < workerNum; i++) {
        preAppendQueue[i] = new ArrayBlockingQueue<ResponseToolKit>(1000);
      }
    }

    private void batchMutate(HRegion.BatchOperationInProgress<?> batchOp,
             final ResponseToolKit responseTK) throws IOException, InterruptedException {
      boolean initialized = false;
      Region.Operation op = batchOp.isInReplay() ?
              Region.Operation.REPLAY_BATCH_MUTATE : Region.Operation.BATCH_MUTATE;
      responseTK.batchOp = batchOp;
      responseTK.hregion.startRegionOperation(op);
      try {
        responseTK.familyMaps = new Map[batchOp.operations.length];
        while (!batchOp.isDone()) {
          if (!batchOp.isInReplay()) {
            responseTK.hregion.checkReadOnly();
          }
          responseTK.hregion.checkResources();
          if (!initialized) {
            responseTK.hregion.writeRequestsCount.increment();
            if (!batchOp.isInReplay()) {
              responseTK.hregion.doPreMutationHook(batchOp);
            }
            initialized = true;
          }
          doMiniBatchMutation(batchOp, responseTK);
        }
      } finally {
        responseTK.hregion.closeRegionOperation(op);
      }
    }

    private void doMiniBatchMutation(HRegion.BatchOperationInProgress<?> batchOp,
                     ResponseToolKit responseTK) throws IOException, InterruptedException {
      HRegion region = responseTK.hregion;
      boolean isInReplay = batchOp.isInReplay();
      // variable to note if all Put items are for the same CF -- metrics related
      boolean putsCfSetConsistent = true;
      //The set of columnFamilies first seen for Put.
      Set<byte[]> putsCfSet = null;
      // variable to note if all Delete items are for the same CF -- metrics related
      boolean deletesCfSetConsistent = true;
      //The set of columnFamilies first seen for Delete.
      Set<byte[]> deletesCfSet = null;

      long currentNonceGroup = HConstants.NO_NONCE, currentNonce = HConstants.NO_NONCE;
      WALEdit walEdit = new WALEdit(isInReplay);
      MultiVersionConsistencyControl.WriteEntry writeEntry = null;
      long txid = -1;
      boolean doRollBackMemstore = false;
      boolean locked = false;
      long addedSize = 0;

      /** Keep track of the locks we hold so we can release them in finally clause */
      List<Region.RowLock> acquiredRowLocks =
              Lists.newArrayListWithCapacity(batchOp.operations.length);
      // reference family maps directly so coprocessors can mutate them if desired
      // We try to set up a batch in the range [firstIndex,lastIndexExclusive)
      int firstIndex = batchOp.nextIndexToProcess;
      int lastIndexExclusive = firstIndex;
      boolean success = false;
      boolean memstoreBegan = false;
      int noOfPuts = 0, noOfDeletes = 0;
      WALKey walKey = null;
      long mvccNum = 0;
      /*** step Barrier ***/
      if (stepBarrier <= 0) {
        batchOp.nextIndexToProcess = batchOp.operations.length;
        responseTK.respond(batchOp.retCodeDetails,
                responseTK.errorThrowable, responseTK.error);
        return;
      }
      try {
        // ------------------------------------
        // STEP 1. Try to acquire as many locks as we can, and ensure
        // we acquire at least one.
        // ----------------------------------
        int numReadyToWrite = 0;
        long now = EnvironmentEdgeManager.currentTime();
        while (lastIndexExclusive < batchOp.operations.length) {
          Mutation mutation = batchOp.getMutation(lastIndexExclusive);
          boolean isPutMutation = mutation instanceof Put;

          Map<byte[], List<Cell>> familyMap = mutation.getFamilyCellMap();
          // store the family map reference to allow for mutations
          responseTK.familyMaps[lastIndexExclusive] = familyMap;

          // skip anything that "ran" already
          if (batchOp.retCodeDetails[lastIndexExclusive].getOperationStatusCode()
                  != HConstants.OperationStatusCode.NOT_RUN) {
            lastIndexExclusive++;
            continue;
          }

          try {
            if (isPutMutation) {
              // Check the families in the put. If bad, skip this one.
              if (isInReplay) {
                region.removeNonExistentColumnFamilyForReplay(familyMap);
              } else {
                region.checkFamilies(familyMap.keySet());
              }
              region.checkTimestamps(mutation.getFamilyCellMap(), now);
            } else {
              region.prepareDelete((Delete) mutation);
            }
            region.checkRow(mutation.getRow(), "doMiniBatchMutation");
          } catch (NoSuchColumnFamilyException nscf) {
            LOG.warn("No such column family in batch mutation", nscf);
            batchOp.retCodeDetails[lastIndexExclusive] = new OperationStatus(
                    HConstants.OperationStatusCode.BAD_FAMILY, nscf.getMessage());
            lastIndexExclusive++;
            continue;
          } catch (FailedSanityCheckException fsce) {
            LOG.warn("Batch Mutation did not pass sanity check", fsce);
            batchOp.retCodeDetails[lastIndexExclusive] = new OperationStatus(
                    HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, fsce.getMessage());
            lastIndexExclusive++;
            continue;
          } catch (WrongRegionException we) {
            LOG.warn("Batch mutation had a row that does not belong to this region", we);
            batchOp.retCodeDetails[lastIndexExclusive] = new OperationStatus(
                    HConstants.OperationStatusCode.SANITY_CHECK_FAILURE, we.getMessage());
            lastIndexExclusive++;
            continue;
          }

          // ignore row lock for hqueue
          if (!region.isHQueueRegion) {
            // If we haven't got any rows in our batch, we should block to get the next one.
            Region.RowLock rowLock = null;
            try {
              rowLock = region.getRowLock(mutation.getRow(), true);
            } catch (IOException ioe) {
              LOG.warn(
                      "Failed getting lock in batch put, row=" + Bytes.toStringBinary(mutation.getRow()),
                      ioe);
            }
            if (rowLock == null) {
              // We failed to grab another lock
              break; // stop acquiring more rows for this batch
            } else {
              acquiredRowLocks.add(rowLock);
            }
          }

          lastIndexExclusive++;
          numReadyToWrite++;

          if (isPutMutation) {
            // If Column Families stay consistent through out all of the
            // individual puts then metrics can be reported as a mutliput across
            // column families in the first put.
            if (putsCfSet == null) {
              putsCfSet = mutation.getFamilyCellMap().keySet();
            } else {
              putsCfSetConsistent = putsCfSetConsistent
                      && mutation.getFamilyCellMap().keySet().equals(putsCfSet);
            }
          } else {
            if (deletesCfSet == null) {
              deletesCfSet = mutation.getFamilyCellMap().keySet();
            } else {
              deletesCfSetConsistent = deletesCfSetConsistent
                      && mutation.getFamilyCellMap().keySet().equals(deletesCfSet);
            }
          }
        }
        /*** step Barrier ***/
        if (stepBarrier <= 1) {
          return;
        }

        // we should record the timestamp only after we have acquired the rowLock,
        // otherwise, newer puts/deletes are not guaranteed to have a newer timestamp
        now = EnvironmentEdgeManager.currentTime();
        byte[] byteNow = Bytes.toBytes(now);

        // Nothing to put/delete -- an exception in the above such as NoSuchColumnFamily?
        if (numReadyToWrite <= 0) return;

        // We've now grabbed as many mutations off the list as we can

        // ------------------------------------
        // STEP 2. Update any LATEST_TIMESTAMP timestamps
        // ----------------------------------
        for (int i = firstIndex; !isInReplay && i < lastIndexExclusive; i++) {
          // skip invalid
          if (batchOp.retCodeDetails[i].getOperationStatusCode()
                  != HConstants.OperationStatusCode.NOT_RUN) continue;

          Mutation mutation = batchOp.getMutation(i);
          if (mutation instanceof Put) {
            region.updateCellTimestamps(responseTK.familyMaps[i].values(), byteNow);
            noOfPuts++;
          } else {
            region.prepareDeleteTimestamps(mutation, responseTK.familyMaps[i], byteNow);
            noOfDeletes++;
          }
          region.rewriteCellTags(responseTK.familyMaps[i], mutation);
        }

        region.lock(region.updatesLock.readLock(), numReadyToWrite);
        locked = true;

        if (region.isHQueueRegion) {
          region.hqueueLock.lock();
          if (!isInReplay) {
            // preassign mvcc for hqueue when not in replay
            writeEntry = region.mvcc.beginMemstoreInsert();
          }
        }
        // calling the pre CP hook for batch mutation
        if (!isInReplay && region.coprocessorHost != null) {
          MiniBatchOperationInProgress<Mutation> miniBatchOp =
                  new MiniBatchOperationInProgress<Mutation>(
                        batchOp.getMutationsForCoprocs(),batchOp.retCodeDetails,
                        batchOp.walEditsFromCoprocessors, firstIndex, lastIndexExclusive);
          if (region.coprocessorHost.preBatchMutate(miniBatchOp)) return;
        }
        /*** step Barrier ***/
        if (stepBarrier <= 2) {
          return;
        }
        // ------------------------------------
        // STEP 3. Build WAL edit
        // ----------------------------------
        for (int i = firstIndex; i < lastIndexExclusive; i++) {
          // Skip puts that were determined to be invalid during preprocessing
          if (batchOp.retCodeDetails[i].getOperationStatusCode() != HConstants.OperationStatusCode.NOT_RUN) {
            continue;
          }

          Mutation m = batchOp.getMutation(i);
          Durability tmpDur = region.getEffectiveDurability(m.getDurability());
          if (tmpDur.ordinal() > responseTK.durability.ordinal()) {
            responseTK.durability = tmpDur;
          }
          if (tmpDur == Durability.SKIP_WAL) {
            region.recordMutationWithoutWal(m.getFamilyCellMap());
            continue;
          }

          long nonceGroup = batchOp.getNonceGroup(i), nonce = batchOp.getNonce(i);
          // In replay, the batch may contain multiple nonces. If so, write WALEdit for each.
          // Given how nonces are originally written, these should be contiguous.
          // They don't have to be, it will still work, just write more WALEdits than needed.
          if (nonceGroup != currentNonceGroup || nonce != currentNonce) {
            if (walEdit.size() > 0) {
              assert isInReplay;
              if (!isInReplay) {
                throw new IOException("Multiple nonces per batch and not in replay");
              }
              // txid should always increase, so having the one from the last call is ok.
              // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
              walKey = new ReplayHLogKey(region.getRegionInfo().getEncodedNameAsBytes(),
                      region.htableDescriptor.getTableName(), now, m.getClusterIds(),
                      currentNonceGroup, currentNonce, region.mvcc);
              walKey.setMultiIndex(firstIndex, lastIndexExclusive);
              walKey.responseTK = responseTK;
              walKey.walEdit = walEdit;
              txid = wal.append(region.htableDescriptor, region.getRegionInfo(), walKey,
                      walEdit, true);
              walEdit = new WALEdit(isInReplay);
              walKey = null;
            }
            currentNonceGroup = nonceGroup;
            currentNonce = nonce;
          }

          // Add WAL edits by CP
          WALEdit fromCP = batchOp.walEditsFromCoprocessors[i];
          if (fromCP != null) {
            for (Cell cell : fromCP.getCells()) {
              walEdit.add(cell);
            }
          }
          region.addFamilyMapToWALEdit(responseTK.familyMaps[i], walEdit);
        }
        /*** step Barrier ***/
        if (stepBarrier <= 3) {
          return;
        }
        // -------------------------
        // STEP 4. Append the final edit to WAL. Do not sync wal.
        // -------------------------
        Mutation mutation = batchOp.getMutation(firstIndex);
        if (isInReplay) {
          // use wal key from the original
          walKey = new ReplayHLogKey(region.getRegionInfo().getEncodedNameAsBytes(),
                  region.htableDescriptor.getTableName(), WALKey.NO_SEQUENCE_ID, now,
                  mutation.getClusterIds(), currentNonceGroup, currentNonce, region.mvcc);
          walKey.setMultiIndex(firstIndex, lastIndexExclusive);
          long replaySeqId = batchOp.getReplaySequenceId();
          walKey.setOrigLogSeqNum(replaySeqId);
          walKey.responseTK = responseTK;
          walKey.walEdit = walEdit;
          if (walEdit.size() > 0) {
            txid = wal.append(region.htableDescriptor, region.getRegionInfo(), walKey, walEdit, true);
          }
        } else {
          if (!region.isHQueueRegion) {
            writeEntry = region.mvcc.beginMemstoreInsert();
          }
          memstoreBegan = true;
          if (walEdit.size() > 0) {
            // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
            walKey = new HLogKey(region.getRegionInfo().getEncodedNameAsBytes(),
                    region.htableDescriptor.getTableName(), WALKey.NO_SEQUENCE_ID, now,
                    mutation.getClusterIds(), currentNonceGroup, currentNonce, region.mvcc);
            walKey.setPreAssignedWriteEntry(writeEntry);
            walKey.setMultiIndex(firstIndex, lastIndexExclusive);
            walKey.responseTK = responseTK;
            walKey.walEdit = walEdit;
            txid = wal.append(region.htableDescriptor, region.getRegionInfo(), walKey, walEdit, true);
          } else {
            // If this is a skip wal operation just get the read point from mvcc
            walKey = new HLogKey(region.getRegionInfo().getEncodedNameAsBytes(),
                    region.getRegionInfo().getTable(), WALKey.NO_SEQUENCE_ID, 0, null,
                    HConstants.NO_NONCE, HConstants.NO_NONCE, region.getMVCC());
            walKey.setMultiIndex(firstIndex, lastIndexExclusive);
            walKey.responseTK = responseTK;
            walKey.walEdit = walEdit;
            txid = region.appendEmptyEdit(wal, writeEntry, walKey, responseTK);
          }
        }

        // unlock for hqueue
        // EHB-401: the correctness of unlocking before invoking FSWALEntry#getWriteEntry depends on
        // the logic that FSHLog#disruptor uses single-thread executor and single handler thus
        // FSWALEntry#stampRegionSequenceId is called sequentially. It's necessary to pay attention to the
        // change of FSHLog#disruptor.
        if (region.isHQueueRegion) {
          region.hqueueLock.unlock();
        }

        // ------------------------------------
        // Acquire the latest mvcc number
        // ----------------------------------
        if (!responseTK.batchOp.isInReplay()) {
          writeEntry = walKey.getWriteEntry();
          mvccNum = writeEntry.getWriteNumber();
        } else {
          mvccNum = responseTK.batchOp.getReplaySequenceId();
        }
        /*** step Barrier ***/
        if (stepBarrier <= 4) {
          walKey = null;
          return;
        }
        // ------------------------------------
        // STEP 5. Write back to memstore
        // Write to memstore. It is ok to write to memstore
        // first without syncing the WAL because we do not roll
        // forward the memstore MVCC. The MVCC will be moved up when
        // the complete operation is done. These changes are not yet
        // visible to scanners till we update the MVCC. The MVCC is
        // moved only when the sync is complete.
        // ----------------------------------
        for (int i = walKey.firstIndex; i < walKey.lastIndexExclusive; i++) {
          if (responseTK.batchOp.retCodeDetails[i].getOperationStatusCode()
                  != HConstants.OperationStatusCode.NOT_RUN) {
            continue;
          }
          doRollBackMemstore = true; // If we have a failure, we need to clean what we wrote
          addedSize += responseTK.hregion.applyFamilyMapToMemstore(
                  responseTK.familyMaps[i], mvccNum, responseTK.batchOp.isInReplay());
        }
        /*** step Barrier ***/
        if (stepBarrier <= 5) {
          walKey = null;
          doRollBackMemstore = false;
          if (responseTK.getRegion().isFlushSize(
                  responseTK.getRegion().addAndGetGlobalMemstoreSize(
                          addedSize))) {
            responseTK.getRegion().requestFlush();
          }
          return;
        }
        // -------------------------------
        // STEP 6. Release row locks, etc.
        // -------------------------------
        if (locked) {
          responseTK.hregion.updatesLock.readLock().unlock();
          locked = false;
        }
        responseTK.hregion.releaseRowLocks(acquiredRowLocks);
        doRollBackMemstore = false;
        /*** step Barrier ***/
        if (stepBarrier <= 6) {
          return;
        }
        success = true;
      } catch (Throwable e) {
        LOG.info("PreAppend throw Exception", e);
        if(walKey != null) walKey.success = false;
        responseTK.errorThrowable = e;
        responseTK.error = StringUtils.stringifyException(e);
      } finally {
        if (doRollBackMemstore) {
          for (int j = 0; j < responseTK.familyMaps.length; j++) {
            for (List<Cell> cells : responseTK.familyMaps[j].values()) {
              responseTK.hregion.rollbackMemstore(cells);
            }
          }
        }

        if (locked) {
          responseTK.hregion.updatesLock.readLock().unlock();
        }
        responseTK.hregion.releaseRowLocks(acquiredRowLocks);

        if (noOfPuts > 0) {
          // There were some Puts in the batch.
          if (region.metricsRegion != null) {
            region.metricsRegion.updatePut();
          }
        }
        if (noOfDeletes > 0) {
          // There were some Deletes in the batch.
          if (region.metricsRegion != null) {
            region.metricsRegion.updateDelete();
          }
        }
        if (walKey != null) {
          // if batch would goes to next stage, no matter it's successful or not,
          // let Finisher stage to advance mvcc
          if (success) walKey.success = true;
          if (responseTK.durability != Durability.ASYNC_WAL &&
                  responseTK.durability != Durability.SKIP_WAL && walKey.success) {
            if (txid < 0) {
              walKey.success = false;
            } else {
              walKey.sequence = txid;
            }
          }
          if (walKey.success) walKey.addedSize = addedSize;
          if (walKey.responseTK.durability == Durability.ASYNC_WAL
                  || walKey.responseTK.durability == Durability.SKIP_WAL
                  || !walKey.success || walEdit.isEmpty()) {
            mutateFinisher.addAsync(walKey);
          } else {
            mutateFinisher.addSync(walKey);
          }
        } else {
          // if the batch cannot goes into next stage, we should
          // 1. advance mvcc, 3. send response
          if(writeEntry != null) responseTK.hregion.mvcc.advanceMemstore(writeEntry);
          if (!responseTK.responded) {
            responseTK.respond(batchOp.retCodeDetails,
                    responseTK.errorThrowable, responseTK.error);
          }
        }
        batchOp.nextIndexToProcess = lastIndexExclusive;
      }
    }

    @Override
    public void runTask(int index) {
      ResponseToolKit responseTK;
      try {
        while (!this.hasInterrupted()) {
          try{
            while (true) {
//              LOG.warn(Thread.currentThread().getName() + ": size = " + pendingSyncs.size());
              responseTK = preAppendQueue[index].take();
              if(responseTK == null) break;
              Mutation[] mutations;
              if(responseTK.mArray != null) {
                mutations = responseTK.mArray;
              } else {
                mutations = new Mutation[] {responseTK.mutation};
              }
              batchMutate( new HRegion.MutationBatch(mutations,
                      HConstants.NO_NONCE, HConstants.NO_NONCE), responseTK);
            }
          } catch (IOException e) {
            LOG.fatal("Error while PreAppend", e);
          }
        }
      } catch (InterruptedException e) {
        LOG.debug(getName(index) + " interrupted while waiting for " +
                "notification from handler thread");
      } catch (Exception e) {
        LOG.error("UNEXPECTED", e);
      } finally {
        LOG.info(getName(index) + " exiting");
      }
    }
  }

  private class FinishProcessor extends AsyncProcessor {

    public ArrayBlockingQueue<WALKey> asyncFinishQueue[];
    public ArrayBlockingQueue<WALKey> syncFinishQueue[];

    public FinishProcessor(String name, int workerNum) {
      super(name, workerNum);
      asyncFinishQueue = new ArrayBlockingQueue[workerNum / 2];
      syncFinishQueue = new ArrayBlockingQueue[workerNum / 2];
      for (int i = 0; i < workerNum / 2; i++) {
        syncFinishQueue[i] = new ArrayBlockingQueue<WALKey>(1000);
        asyncFinishQueue[i] = new ArrayBlockingQueue<WALKey>(1000);
      }
    }

    public void addAsync(WALKey walKey) throws InterruptedException {
      int num = getThreadIndex(walKey.responseTK.hregion, asyncFinishQueue.length);
      this.asyncFinishQueue[num].put(walKey);
    }

    public void addSync(WALKey walKey) throws InterruptedException {
      int num = getThreadIndex(walKey.responseTK.hregion, syncFinishQueue.length);
      this.syncFinishQueue[num].put(walKey);
    }

    public void finishMutate(WALKey walKey) throws IOException {
      MultiVersionConsistencyControl.WriteEntry writeEntry = walKey.preAssignedWriteEntry;
      HRegion hregion = walKey.responseTK.hregion;
      HRegion.BatchOperationInProgress batchOp = walKey.responseTK.batchOp;
      long mvccNum;
      try {
        if (!walKey.success) {
          // if something wrong happend before, not during current stage,
          // that means we should roll back memstore.
//          for (int j = 0; j < walKey.responseTK.familyMaps.length; j++) {
//            for (List<Cell> cells : walKey.responseTK.familyMaps[j].values()) {
//              hregion.rollbackMemstore(cells);
//            }
//          }
        } else {

          if (!batchOp.isInReplay()) {
            mvccNum = writeEntry.getWriteNumber();
          } else {
            mvccNum = batchOp.getReplaySequenceId();
          }
          // calling the post CP hook for batch mutation
          if (!batchOp.isInReplay() && hregion.coprocessorHost != null) {
            MiniBatchOperationInProgress<Mutation> miniBatchOp =
                    new MiniBatchOperationInProgress<Mutation>(batchOp.getMutationsForCoprocs(),
                            batchOp.retCodeDetails, batchOp.walEditsFromCoprocessors,
                            walKey.firstIndex, walKey.lastIndexExclusive);
            hregion.coprocessorHost.postBatchMutate(miniBatchOp);
          }
          /*** step Barrier ***/
          if (stepBarrier <= 7) {
            return;
          }
          // ------------------------------------------------------------------
          // STEP 8. Advance mvcc. This will make this put visible to scanners and getters.
          // ------------------------------------------------------------------
          if (writeEntry != null) {
            hregion.mvcc.completeMemstoreInsert(writeEntry);
            writeEntry = null;
          } else if (batchOp.isInReplay()) {
            // ensure that the sequence id of the region is at least as big as orig log seq id
            hregion.mvcc.advanceTo(mvccNum);
          }
          for (int i = walKey.firstIndex; i < walKey.lastIndexExclusive; i++) {
            if (batchOp.retCodeDetails[i] == OperationStatus.NOT_RUN) {
              batchOp.retCodeDetails[i] = OperationStatus.SUCCESS;
            }
          }
          /*** step Barrier ***/
          if (stepBarrier <= 8) {
            return;
          }
          // ------------------------------------
          // STEP 9. Run coprocessor post hooks. This should be done after the wal is
          // synced so that the coprocessor contract is adhered to.
          // ------------------------------------
          if (!batchOp.isInReplay() && hregion.coprocessorHost != null) {
            for (int i = walKey.firstIndex; i < walKey.lastIndexExclusive; i++) {
              // only for successful puts
              if (batchOp.retCodeDetails[i].getOperationStatusCode()
                      != HConstants.OperationStatusCode.SUCCESS) {
                continue;
              }
              Mutation m = batchOp.getMutation(i);
              if (m instanceof Put) {
                hregion.coprocessorHost.postPut((Put) m, walKey.walEdit, m.getDurability());
              } else {
                hregion.coprocessorHost.postDelete((Delete) m, walKey.walEdit, m.getDurability());
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.info("FinishProcessor Throw Exception", e);
        walKey.success = false;
        // keep previous induced exception if any
        if (walKey.responseTK.errorThrowable == null) {
          walKey.responseTK.errorThrowable = e;
          walKey.responseTK.error = StringUtils.stringifyException(e);
        }
      } finally {
        // if the wal sync was unsuccessful, remove keys from memstore
        if (!walKey.success) {
          if (writeEntry != null) hregion.mvcc.advanceMemstore(writeEntry);
        } else if (writeEntry != null) {
          hregion.mvcc.completeMemstoreInsert(writeEntry);
        }
        // See if the column families were consistent through the whole thing.
        // if they were then keep them. If they were not then pass a null.
        // null will be treated as unknown.
        // Total time taken might be involving Puts and Deletes.
        // Split the time for puts and deletes based on the total number of Puts and Deletes.

        if (!walKey.success) {
          for (int i = walKey.firstIndex; i < walKey.lastIndexExclusive; i++) {
            if (batchOp.retCodeDetails[i].getOperationStatusCode()
                    == HConstants.OperationStatusCode.NOT_RUN) {
              batchOp.retCodeDetails[i] = OperationStatus.FAILURE;
            }
          }
        }
        if (hregion.coprocessorHost != null && !batchOp.isInReplay()) {
          // call the coprocessor hook to do any finalization steps
          // after the put is done
          MiniBatchOperationInProgress<Mutation> miniBatchOp =
                  new MiniBatchOperationInProgress<Mutation>(
                        batchOp.getMutationsForCoprocs(),batchOp.retCodeDetails,
                        batchOp.walEditsFromCoprocessors, walKey.firstIndex, walKey.lastIndexExclusive);
          hregion.coprocessorHost.postBatchMutateIndispensably(miniBatchOp, walKey.success);
        }
        if (walKey.responseTK.getRegion().isFlushSize(
                walKey.responseTK.getRegion().addAndGetGlobalMemstoreSize(
                        walKey.addedSize))) {
          walKey.responseTK.getRegion().requestFlush();
        }
        if (!walKey.responseTK.responded && (walKey.responseTK.errorThrowable != null
                || batchOp.operations.length <= walKey.lastIndexExclusive)) {
          walKey.responseTK.respond(batchOp.retCodeDetails,
                  walKey.responseTK.errorThrowable, walKey.responseTK.error);
        }
      }
    }

    public void waitForSync(long sequence) throws InterruptedException {
      if (hlog != null) {
        if (hlog.getHighestSyncedSequence() < sequence) {
          // TODO a better method to wake it up
          synchronized (hlog.syncer.hrgbNotifyLock) {
            hlog.syncer.finishProcessorNeedWakeUp++;
            while (hlog.getHighestSyncedSequence() < sequence) {
              hlog.syncer.notifySyncLock();
              hlog.syncer.hrgbNotifyLock.wait();
            }
            hlog.syncer.finishProcessorNeedWakeUp--;
          }
        }
      }
    }

    @Override
    public void runTask(int index) {
      WALKey walKey;
      try {
        while (!this.hasInterrupted()) {
          this.isWorkerProcessing[index] = true;
          try {
            while (true) {
              if(index % 2 == 0) {
                walKey = syncFinishQueue[index / 2].take();
              } else {
                walKey = asyncFinishQueue[index / 2].take();
              }
              if(walKey == null) break;
              if(index % 2 == 0 && walKey.success) {
                waitForSync(walKey.sequence);
              }
              finishMutate(walKey);
            }
          } catch (InterruptedException e) {
            LOG.info("Interrupted while FinishProcessor worker thread run ");
            this.isWorkerProcessing[index] = false;
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            LOG.fatal("Error while FinishProcessor worker thread run " + index, e);
            this.isWorkerProcessing[index]  = false;
          }
        }
      } catch (Exception e) {
        LOG.error("UNEXPECTED", e);
      } finally {
        LOG.info(getName(index) + " worker exiting");
      }
    }
  }

  public abstract class AsyncProcessor<T> extends HasThreads {

    protected volatile boolean isProcessing = false;
    protected volatile boolean[] isWorkerProcessing;

    public AsyncProcessor(String name) {
      super(name);
    }

    public AsyncProcessor(String name, int workerNum) {
      super(name, workerNum);
      isWorkerProcessing = new boolean[workerNum];
    }

    public boolean isProcessiong() {
      return this.isProcessing;
    }

    public void runTask(int index) {

    }
  }

}
