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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Action;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

public class MultiMutateResponseConstructor extends AbstractMultiResponseConstructor {
  private static final Log LOG = LogFactory.getLog(MultiMutateResponseConstructor.class);

  protected AtomicInteger taskCounter = new AtomicInteger(0);
  protected HRegionServer regionServer;
  protected RegionActionResult[] regionActionResultStore;
  protected RegionActionResult.Builder regionActionResultBuilder = RegionActionResult.newBuilder();

  public MultiMutateResponseConstructor(CallRunner callTask, HRegionServer rs, int regionActionCounts) {
    super(callTask);
    this.regionServer = rs;
    regionActionResultStore = new RegionActionResult[regionActionCounts];
    setTaskCounter(regionActionCounts);
  }

  public void setTaskCounter(int actionCounts){
    taskCounter.set(actionCounts);
  }

  public synchronized void setExceptionMutateAndTryResponse(IOException e,
                                      RegionAction regionAction, int regionActionIndex){
    regionActionResultBuilder.clear();
    regionActionResultBuilder.setException(ResponseConverter.buildException(e));
    regionActionResultStore[regionActionIndex] = regionActionResultBuilder.build();
    // All Mutations in this RegionAction not executed as we can not see the Region online here
    // in this RS. Will be retried from Client. Skipping all the Cells in CellScanner
    // corresponding to these Mutations.
    callTask.getRpcServer().getRsRpcServices().skipCellsForMutations(
            regionAction.getActionList(), callTask.getRpcCall().getCellScanner());
    checkResponse();
  }

  private void checkResponse() {
    if (taskCounter.decrementAndGet() == 0) {
      for (RegionActionResult regionActionResult : regionActionResultStore) {
        if (regionActionResult == null) {
          LOG.warn("Not enough regionAction! should have: " + regionActionResultStore.length);
        }
        responseBuilder.addRegionActionResult(regionActionResult);
      }
      doResponse();
    }
  }
  public synchronized void finishMutateAndTryResponse(OperationStatus[] codes,List<Action> mutationActions,
                                    HRegion hregion, long startTime,boolean batchContainsPuts,
                                    boolean batchContainsDelete, Throwable errorThrowable, int regionActionIndex) {
    //TODO Check all task finished and then do response
    if (taskCounter.get() == 0) {
      LOG.warn("TaskCounter is 0 already, duplicated build!");
      return;
    }
    regionActionResultBuilder.clear();
    if (errorThrowable == null) {
      for (int i = 0; i < codes.length; i++) {
        int index = mutationActions.get(i).getIndex();
        Exception e = null;
        switch (codes[i].getOperationStatusCode()) {
          case BAD_FAMILY:
            e = new NoSuchColumnFamilyException(codes[i].getExceptionMsg());
            regionActionResultBuilder.addResultOrException(getResultOrException(e, index));
            break;

          case SANITY_CHECK_FAILURE:
            e = new FailedSanityCheckException(codes[i].getExceptionMsg());
            regionActionResultBuilder.addResultOrException(getResultOrException(e, index));
            break;

          default:
            e = new DoNotRetryIOException(codes[i].getExceptionMsg());
            regionActionResultBuilder.addResultOrException(getResultOrException(e, index));
            break;

          case SUCCESS:
            regionActionResultBuilder.addResultOrException(getResultOrException(
                    ClientProtos.Result.getDefaultInstance(), index,
                    hregion.getRegionStats()));
            break;
        }
      }
    } else {
      for (int i = 0; i < mutationActions.size(); i++) {
        regionActionResultBuilder.addResultOrException(
                getResultOrException((Exception) errorThrowable, mutationActions.get(i).getIndex()));
      }
    }
    if (regionServer.metricsRegionServer != null) {
      long after = EnvironmentEdgeManager.currentTime();
      if (batchContainsPuts) {
        regionServer.metricsRegionServer.updatePut(after - startTime);
      }
      if (batchContainsDelete) {
        regionServer.metricsRegionServer.updateDelete(after - startTime);
      }
    }
    if (regionActionResultStore[regionActionIndex] != null) {
      LOG.warn("Duplicated actionResult for region: " + hregion.getRegionInfo());
    } else {
      regionActionResultStore[regionActionIndex] = regionActionResultBuilder.build();
    }
    checkResponse();
  }

  private static ClientProtos.ResultOrException getResultOrException(
          final ClientProtos.Result r, final int index, final ClientProtos.RegionLoadStats stats) {
    return getResultOrException(ResponseConverter.buildActionResult(r, stats), index);
  }

  private static ClientProtos.ResultOrException getResultOrException(final Exception e, final int index) {
    return getResultOrException(ResponseConverter.buildActionResult(e), index);
  }

  private static ClientProtos.ResultOrException getResultOrException(
          final ClientProtos.ResultOrException.Builder builder, final int index) {
    return builder.setIndex(index).build();
  }

  @Override
  public void cleanup() {
    this.callTask = null;
    this.regionServer = null;
    this.cellScanner = null;
    this.responseBuilder = null;
  }
}
