/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemstoreSize;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.ReplayHLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MutatePreAppendTask extends MutateTask {
  private static final Log LOG = LogFactory.getLog(MutatePreAppendTask.class);

  public MutatePreAppendTask(CallRunner callRunner, MutateRequest mutate, MutationProto mutationProto, MutationType type) {
    super(callRunner, mutate, mutationProto, type);
  }

  public MutatePreAppendTask(MultiMutateResponseConstructor multiConstructor, Region hregion,
                             List<ClientProtos.Action> mutationActions, RpcCall call, int regionActionIndex) throws IOException {
    super(multiConstructor, hregion, mutationActions, call, regionActionIndex);
  }

  @Override
  public void run() {
    try {
      startTime = System.currentTimeMillis();
      batchMutate( new HRegion.MutationBatch(mutations, HConstants.NO_NONCE, HConstants.NO_NONCE));
//      fakeResponse(new HRegion.MutationBatch(mutations, HConstants.NO_NONCE, HConstants.NO_NONCE));
    } catch (Exception e) {
      LOG.warn(MutatePreAppendTask.class.getName() + "exception", e);
    }
  }

  /**
   * Function for debugging or testing. Reponse instantly after taken from a queue.
   * **/
  public void fakeResponse(HRegion.BatchOperationInProgress<?> batchOp){
    for (int i = 0; i < batchOp.retCodeDetails.length; i++) {
      if (batchOp.retCodeDetails[i] == OperationStatus.NOT_RUN) {
        batchOp.retCodeDetails[i] = OperationStatus.SUCCESS;
      }
    }
    try {
      onSuccess(batchOp.retCodeDetails);
    } catch (Exception e) {
      LOG.fatal("repsonse exception",e);
    }
  }

  private void batchMutate(HRegion.BatchOperationInProgress<?> batchOp) throws IOException, InterruptedException {
    boolean initialized = false;
    Region.Operation op = batchOp.isInReplay() ?
            Region.Operation.REPLAY_BATCH_MUTATE : Region.Operation.BATCH_MUTATE;
    hregion.startRegionOperation(op);
    try {
      while (!batchOp.isDone()) {
        if (!batchOp.isInReplay()) {
          hregion.checkReadOnly();
        }
        hregion.checkResources();
        if (!initialized) {
          hregion.writeRequestsCount.increment();
          if (!batchOp.isInReplay()) {
            hregion.doPreMutationHook(batchOp);
          }
          initialized = true;
        }
        doMiniBatchMutation(batchOp);
      }
    } finally {
      hregion.closeRegionOperation(op);
    }
  }

  private void doMiniBatchMutation(HRegion.BatchOperationInProgress<?> batchOp)
          throws IOException, InterruptedException {
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
//    WALEdit walEdit = new WALEdit(isInReplay);
    WALEdit walEdit = null;
    MultiVersionConsistencyControl.WriteEntry writeEntry = null;
    int cellCount = 0;
    long txid = -1;
    boolean doRollBackMemstore = false;
    boolean locked = false;
//    long addedSize = 0;

    /** Keep track of the locks we hold so we can release them in finally clause */
    List<RowLock> acquiredRowLocks =
            Lists.newArrayListWithCapacity(batchOp.operations.length);
    // reference family maps directly so coprocessors can mutate them if desired
    Map<byte[], List<Cell>>[] familyMaps = new Map[batchOp.operations.length];
    // reference family maps directly so coprocessors can mutate them if desired
    // We try to set up a batch in the range [firstIndex,lastIndexExclusive)
    int firstIndex = batchOp.nextIndexToProcess;
    int lastIndexExclusive = firstIndex;
    boolean success = false;
    boolean memstoreBegan = false;
    int noOfPuts = 0, noOfDeletes = 0;
    WALKey walKey = null;
    Throwable errorThrowable = null;
    long mvccNum = 0;
    Durability durability = Durability.USE_DEFAULT;
    MemstoreSize memstoreSize = new MemstoreSize();
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
        familyMaps[lastIndexExclusive] = familyMap;
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
              hregion.removeNonExistentColumnFamilyForReplay(familyMap);
            } else {
              hregion.checkFamilies(familyMap.keySet());
            }
            hregion.checkTimestamps(mutation.getFamilyCellMap(), now);
          } else {
            hregion.prepareDelete((Delete) mutation);
          }
          hregion.checkRow(mutation.getRow(), "doMiniBatchMutation");
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
        if (!hregion.isHQueueRegion()) {
          // If we haven't got any rows in our batch, we should block to get the next one.
          RowLock rowLock = null;
          try {
            rowLock = hregion.getRowLock(mutation.getRow(), true);
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

        if (isInReplay) {
          for (List<Cell> cells : mutation.getFamilyCellMap().values()) {
            cellCount += cells.size();
          }
        }

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
          hregion.updateCellTimestamps(familyMaps[i].values(), byteNow);
          noOfPuts++;
        } else {
          hregion.prepareDeleteTimestamps(mutation, familyMaps[i], byteNow);
          noOfDeletes++;
        }
        hregion.rewriteCellTags(familyMaps[i], mutation);
        WALEdit fromCP = batchOp.walEditsFromCoprocessors[i];
        if (fromCP != null) {
          cellCount += fromCP.size();
        }
        for (List<Cell> cells : familyMaps[i].values()) {
          cellCount += cells.size();
        }
      }
      walEdit = new WALEdit(cellCount, isInReplay);
      hregion.lock(hregion.getUpdatesLock().readLock(), numReadyToWrite);
      locked = true;

      if (hregion.isHQueueRegion()) {
        hregion.hqueueLock.lock();
        if (!isInReplay) {
          // preassign mvcc for hqueue when not in replay
          writeEntry = hregion.getMVCC().beginMemstoreInsert();
        }
      }
      // calling the pre CP hook for batch mutation
      if (!isInReplay && hregion.getCoprocessorHost() != null) {
        MiniBatchOperationInProgress<Mutation> miniBatchOp =
                new MiniBatchOperationInProgress<Mutation>(
                        batchOp.getMutationsForCoprocs(),batchOp.retCodeDetails,
                        batchOp.walEditsFromCoprocessors, firstIndex, lastIndexExclusive);
        if (hregion.getCoprocessorHost().preBatchMutate(miniBatchOp)) return;
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
        Durability tmpDur = hregion.getEffectiveDurability(m.getDurability());
        if (tmpDur.ordinal() > durability.ordinal()) {
          durability = tmpDur;
        }
        if (tmpDur == Durability.SKIP_WAL) {
          hregion.recordMutationWithoutWal(m.getFamilyCellMap());
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
            walKey = new ReplayHLogKey(hregion.getRegionInfo().getEncodedNameAsBytes(),
                    hregion.getHtableDescriptor().getTableName(), now, m.getClusterIds(),
                    currentNonceGroup, currentNonce, hregion.getMVCC(), hregion.getReplicationScope());
            txid = wal.append(hregion.getRegionInfo(), walKey,
                    walEdit, true);
            walEdit = new WALEdit(cellCount, isInReplay);
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
        hregion.addFamilyMapToWALEdit(familyMaps[i], walEdit);
      }
      // -------------------------
      // STEP 4. Append the final edit to WAL. Do not sync wal.
      // -------------------------
      Mutation mutation = batchOp.getMutation(firstIndex);
      if (isInReplay) {
        // use wal key from the original
        walKey = new ReplayHLogKey(hregion.getRegionInfo().getEncodedNameAsBytes(),
                hregion.getHtableDescriptor().getTableName(), WALKey.NO_SEQUENCE_ID, now,
                mutation.getClusterIds(), currentNonceGroup, currentNonce, hregion.getMVCC()
        , hregion.getReplicationScope());
        long replaySeqId = batchOp.getReplaySequenceId();
        walKey.setOrigLogSeqNum(replaySeqId);
        if (walEdit.size() > 0) {
          txid = this.wal.append(hregion.getRegionInfo(), walKey, walEdit, true);
        }
      } else {
        if (walEdit.size() > 0) {
          // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
          walKey = new HLogKey(hregion.getRegionInfo().getEncodedNameAsBytes(),
                  hregion.getHtableDescriptor().getTableName(), WALKey.NO_SEQUENCE_ID, now,
                  mutation.getClusterIds(), currentNonceGroup, currentNonce, hregion.getMVCC()
          , hregion.getReplicationScope());
          txid = this.wal.append(hregion.getRegionInfo(), walKey, walEdit, true);
        } else {
          // If this is a skip wal operation just get the read point from mvcc
          walKey = new HLogKey(hregion.getRegionInfo().getEncodedNameAsBytes(),
                  hregion.getRegionInfo().getTable(), WALKey.NO_SEQUENCE_ID, 0, null,
                  HConstants.NO_NONCE, HConstants.NO_NONCE, hregion.getMVCC()
                  , hregion.getReplicationScope());
          txid = hregion.appendEmptyEdit(this.wal, walKey);
        }
      }

      // unlock for hqueue
      // EHB-401: the correctness of unlocking before invoking FSWALEntry#getWriteEntry depends on
      // the logic that FSHLog#disruptor uses single-thread executor and single handler thus
      // FSWALEntry#stampRegionSequenceId is called sequentially. It's necessary to pay attention to the
      // change of FSHLog#disruptor.
      if (hregion.isHQueueRegion()) {
        hregion.hqueueLock.unlock();
      }

      // ------------------------------------
      // Acquire the latest mvcc number
      // ----------------------------------
      if (!batchOp.isInReplay()) {
        writeEntry = walKey.getWriteEntry();
        mvccNum = writeEntry.getWriteNumber();
      } else {
        mvccNum = batchOp.getReplaySequenceId();
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
      for (int i = firstIndex; i < lastIndexExclusive; i++) {
        if (batchOp.retCodeDetails[i].getOperationStatusCode()
                != HConstants.OperationStatusCode.NOT_RUN) {
          continue;
        }
        doRollBackMemstore = true; // If we have a failure, we need to clean what we wrote
        // We need to update the sequence id for following reasons.
        // 1) If the op is in replay mode, FSWALEntry#stampRegionSequenceId won't stamp sequence id.
        // 2) If no WAL, FSWALEntry won't be used
        boolean updateSeqId = isInReplay || batchOp.getMutation(i).getDurability() == Durability.SKIP_WAL;
        if (updateSeqId) {
          hregion.updateSequenceId(familyMaps[i].values(),isInReplay? batchOp.getReplaySequenceId(): writeEntry.getWriteNumber());
        }
        hregion.applyFamilyMapToMemstore(familyMaps[i], memstoreSize);
      }
      // -------------------------------
      // STEP 6. Release row locks, etc.
      // -------------------------------
      if (locked) {
        hregion.getUpdatesLock().readLock().unlock();
        locked = false;
      }
      hregion.releaseRowLocks(acquiredRowLocks);
      doRollBackMemstore = false;
      success = true;
    } catch (Throwable e) {
      LOG.info("PreAppend throw Exception", e);
      success = false;
      errorThrowable = e;
    } finally {
      // if the wal sync was unsuccessful, remove keys from memstore
      if (doRollBackMemstore) {
        for (int j = 0; j < familyMaps.length; j++) {
          for (List<Cell> cells : familyMaps[j].values()) {
            hregion.rollbackMemstore(cells);
          }
        }
      }

      if (locked) {
        hregion.getUpdatesLock().readLock().unlock();
      }
      hregion.releaseRowLocks(acquiredRowLocks);

      if (noOfPuts > 0) {
        // There were some Puts in the batch.
        if (hregion.metricsRegion != null) {
          hregion.metricsRegion.updatePut();
        }
      }
      if (noOfDeletes > 0) {
        // There were some Deletes in the batch.
        if (hregion.metricsRegion != null) {
          hregion.metricsRegion.updateDelete();
        }
      }
      MutateFinishTask mutateFinishTask = new MutateFinishTask(this, batchOp,
              familyMaps, firstIndex, lastIndexExclusive, success, txid, memstoreSize,
              errorThrowable, durability, writeEntry, walEdit, startTime, mutationActions,
              batchContainsPuts, batchContainsDelete, multiConstructor, regionActionIndex);
      // we pass failed task to next stage to process as well,
      // due to higher workload of current stage
      if (durability == Durability.ASYNC_WAL
              || durability == Durability.SKIP_WAL) {
        forward(Stage.FinishAsyncStage, mutateFinishTask);
      } else {
        forward(Stage.FinishSyncStage, mutateFinishTask);
      }
      batchOp.nextIndexToProcess = lastIndexExclusive;
    }
  }
}
