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
import com.sun.org.apache.xpath.internal.operations.Mult;
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
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemstoreSize;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl.WriteEntry;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.ReplayHLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MutateFinishTask extends MutateTask {
  private static final Log LOG = LogFactory.getLog(MutateFinishTask.class);
  private Map<byte[], List<Cell>>[] familyMaps;
  private volatile int firstIndex;
  private volatile int lastIndexExclusive;
  private volatile boolean success = false;
  private volatile long sequence;
  private MemstoreSize memstoreSize;
  private HRegion.BatchOperationInProgress<?> batchOp;
  private Durability durability;
  private boolean rollbackMemstore;
  private WriteEntry writeEntry;
  private WALEdit walEdit;
  private MutateTask mutateTask;

  public MutateFinishTask(MutateTask mutateTask, HRegion.BatchOperationInProgress<?> batchOp, Map<byte[],
          List<Cell>>[] familyMaps, int firstIndex, int lastIndexExclusive, boolean success, long sequence,
          MemstoreSize memstoreSize, Throwable errorThrowable, Durability durability, WriteEntry writeEntry,
          WALEdit walEdit, long startTime, List<ClientProtos.Action> mutationActions, boolean batchContainsPuts,
          boolean batchContainsDelete, MultiMutateResponseConstructor multiConstructor, int regionActionIndex) {
    this.mutateTask = mutateTask;
    this.mutateRequest = mutateTask.mutateRequest;
    this.callRunner = mutateTask.callRunner;
    this.rpcCall = mutateTask.rpcCall;
    this.hregion = mutateTask.hregion;
    this.errorThrowable = mutateTask.errorThrowable;
    this.batchOp = batchOp;
    this.familyMaps =familyMaps;
    this.firstIndex = firstIndex;
    this.lastIndexExclusive = lastIndexExclusive;
    this.success = success;
    this.sequence = sequence;
    this.memstoreSize = memstoreSize;
    this.durability = durability;
    this.writeEntry = writeEntry;
    this.walEdit = walEdit;
    this.startTime = startTime;
    this.mutationActions = mutationActions;
    this.batchContainsPuts = batchContainsPuts;
    this.batchContainsDelete = batchContainsDelete;
    this.multiConstructor = multiConstructor;
    this.regionActionIndex = regionActionIndex;
  }

  @Override
  public void run() {
    try {
      if (getCurrentStage().getName().equals(Stage.FinishSyncStage)) {
        try {
          if (success && !walEdit.isEmpty() &&  sequence >= 0) {
            waitForSync(sequence);
          }
        } catch (IOException ioe){
          rollbackMemstore = true;
          if (errorThrowable != null) {
            throw ioe;
          } else {
            errorThrowable = ioe;
          }
        }
      }
      finishMutate(success, rollbackMemstore);
    } catch (Exception e) {
      LOG.warn(MutateFinishTask.class.getName() + "exception", e);
    }
  }

  public void waitForSync(long sequence) throws InterruptedException, IOException {
    if (sequence > 0) {
      hregion.syncOrDefer(sequence, durability);
    } else {
      throw new IOException("FinishSyncQueue has sequence less than 1");
    }
  }

  public void finishMutate(boolean success, boolean rollbackMemstore) throws IOException {
    long mvccNum;
    try {
      if (success && rollbackMemstore) {
        // if preAppend execute nicely but sync failed
        for (int j = 0; j < familyMaps.length; j++) {
          for (List<Cell> cells : familyMaps[j].values()) {
            hregion.rollbackMemstore(cells);
          }
        }
        success = false;
      } else if (success) {
        if (!batchOp.isInReplay()) {
          mvccNum = writeEntry.getWriteNumber();
        } else {
          mvccNum = batchOp.getReplaySequenceId();
        }
        // calling the post CP hook for batch mutation
        if (!batchOp.isInReplay() && hregion.getCoprocessorHost() != null) {
          MiniBatchOperationInProgress<Mutation> miniBatchOp =
                  new MiniBatchOperationInProgress<Mutation>(batchOp.getMutationsForCoprocs(),
                          batchOp.retCodeDetails, batchOp.walEditsFromCoprocessors,
                          firstIndex, lastIndexExclusive);
          hregion.getCoprocessorHost().postBatchMutate(miniBatchOp);
        }
        // ------------------------------------------------------------------
        // STEP 8. Advance mvcc. This will make this put visible to scanners and getters.
        // ------------------------------------------------------------------
        if (writeEntry != null) {
          hregion.getMVCC().completeMemstoreInsert(writeEntry);
          writeEntry = null;
        } else if (batchOp.isInReplay()) {
          // ensure that the sequence id of the region is at least as big as orig log seq id
          hregion.getMVCC().advanceTo(mvccNum);
        }
        for (int i = firstIndex; i < lastIndexExclusive; i++) {
          if (batchOp.retCodeDetails[i] == OperationStatus.NOT_RUN) {
            batchOp.retCodeDetails[i] = OperationStatus.SUCCESS;
          }
        }
        // ------------------------------------
        // STEP 9. Run coprocessor post hooks. This should be done after the wal is
        // synced so that the coprocessor contract is adhered to.
        // ------------------------------------
        if (!batchOp.isInReplay() && hregion.getCoprocessorHost() != null) {
          for (int i = firstIndex; i < lastIndexExclusive; i++) {
            // only for successful puts
            if (batchOp.retCodeDetails[i].getOperationStatusCode()
                    != HConstants.OperationStatusCode.SUCCESS) {
              continue;
            }
            Mutation m = batchOp.getMutation(i);
            if (m instanceof Put) {
              hregion.getCoprocessorHost().postPut((Put) m, walEdit, m.getDurability());
            } else {
              hregion.getCoprocessorHost().postDelete((Delete) m, walEdit, m.getDurability());
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.info("FinishProcessor Throw Exception", e);
      success = false;
      // keep previous induced exception if any
      if (errorThrowable == null) {
        errorThrowable = e;
      }
    } finally {
      // if the wal sync was unsuccessful, remove keys from memstore
      if (!success) {
        if (writeEntry != null) hregion.getMVCC().advanceMemstore(writeEntry);
        for (int i = firstIndex; i < lastIndexExclusive; i++) {
          if (batchOp.retCodeDetails[i].getOperationStatusCode()
                  == HConstants.OperationStatusCode.NOT_RUN) {
            batchOp.retCodeDetails[i] = OperationStatus.FAILURE;
          }
        }
      } else {
          hregion.addAndGetGlobalMemstoreSize(memstoreSize);
        if (writeEntry != null) {
          hregion.getMVCC().completeMemstoreInsert(writeEntry);
        }
      }

      if (hregion.getCoprocessorHost() != null && !batchOp.isInReplay()) {
        // call the coprocessor hook to do any finalization steps
        // after the put is done
        MiniBatchOperationInProgress<Mutation> miniBatchOp =
                new MiniBatchOperationInProgress<Mutation>(
                        batchOp.getMutationsForCoprocs(),batchOp.retCodeDetails,
                        batchOp.walEditsFromCoprocessors, firstIndex, lastIndexExclusive);
        hregion.getCoprocessorHost().postBatchMutateIndispensably(miniBatchOp, success);
      }
      if (hregion.isFlushSize(hregion.getMemstoreSize())) {
        hregion.requestFlush();
      }
      if (!this.mutateTask.mutateFinished) {
        if (errorThrowable != null) {
          this.mutateTask.mutateFinished = true;
          onError(batchOp.retCodeDetails);
        } else if (batchOp.operations.length <= lastIndexExclusive) {
          this.mutateTask.mutateFinished = true;
          onSuccess(batchOp.retCodeDetails);
        }
      }
    }
  }

  @Override
  protected void cleanup() {
    super.cleanup();
    this.mutateTask.cleanup();
    this.mutateTask = null;
    this.hregion =null;
    this.batchOp = null;
    this.familyMaps = null;
    this.durability = null;
    this.writeEntry = null;
    this.walEdit = null;
  }

}
