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
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Action;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponseOrBuilder;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
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
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.Message;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TH;

public abstract class MutateTask extends AbstractTask {
  private static final Log LOG = LogFactory.getLog(MutateTask.class);
  protected CallRunner callRunner;
  protected MutateRequest mutateRequest;
  protected MultiMutateResponseConstructor multiConstructor;
  protected HRegion hregion;
  protected WAL wal;
  protected Mutation[] mutations;
  protected List<ClientProtos.Action> mutationActions;
  protected Throwable errorThrowable;
  protected volatile boolean mutateFinished;
  protected boolean batchContainsPuts;
  protected boolean batchContainsDelete;
  public Mutation[] getMutations () {
    return mutations;
  }
  public long startTime;
  // default regionActionIndex is 0
  protected int regionActionIndex = 0;

  public MutateTask(CallRunner callRunner, MutateRequest mutate, MutationProto mutationProto, MutationType type) {
    this.callRunner = callRunner;
    this.mutateRequest = mutate;
    this.rpcCall = callRunner.getRpcCall();
    Mutation mutation = parseMutate(mutationProto, type);
    if (mutation != null) this.mutations = new Mutation[] {mutation};
    this.wal = hregion.getWAL();
  }

  public MutateTask(MultiMutateResponseConstructor multiConstructor, Region region,
            List<ClientProtos.Action> mutationActions, RpcCall call, int regionActionIndex) throws IOException {
    this.rpcCall = call;
    this.multiConstructor = multiConstructor;
    this.hregion = (HRegion) region;
    this.mutationActions = mutationActions;
    this.mutations = getmArray(call, mutationActions);
    this.wal = hregion.getWAL();
    this.regionActionIndex = regionActionIndex;
  }

  public MutateTask() {

  }
  /**
   * Parse mutate and do respond immediately if anything wrong.
   *
   * @param mutation the MutationProto instance
   * @param type the mutation type
   */
  protected Mutation parseMutate(MutationProto mutation, MutationType type) {
    try {
      this.hregion = (HRegion) callRunner.getRpcServer()
              .getRsRpcServices().getRegion(mutateRequest.getRegion());
      return callRunner.getRpcServer().getRsRpcServices()
              .sedaMutate(this.mutateRequest, mutation,
                      callRunner.getRpcCall().getCellScanner(), type);
    } catch (Throwable e) {
      LOG.warn(Thread.currentThread().getName()
              + ": parsing Mutate:" + callRunner.getRpcCall().toShortString(), e);
      if (e instanceof Error) {
        throw (Error)e;
      }
      errorThrowable = e;
//      TODO onError();
    }
    return null;
  }

  protected void onError(OperationStatus[] retCodeDetails) {
    mutateFinished = true;
    if (multiConstructor == null) {
      try {
        doResponse(null,null,
                StringUtils.stringifyException(errorThrowable));
      } catch (IOException e) {
        LOG.debug("onError response failed: ", e);
      } finally {
        this.callRunner.getRpcServer().addCallSize(this.rpcCall.getSize() * -1);
        this.callRunner.cleanup();
        this.cleanup();
      }
    } else {
      try {
        multiConstructor.finishMutateAndTryResponse(retCodeDetails,mutationActions, hregion,
                startTime, batchContainsPuts, batchContainsDelete, errorThrowable, regionActionIndex);
      } catch (Exception e) {
        LOG.warn("multiTask.finishMutateAndTryResponse Exception:", e);
      } finally {
        this.cleanup();
      }
    }
  }

  protected void onSuccess(OperationStatus[] retCodeDetails) throws IOException {
    // if has no multiTask means the
    mutateFinished = true;
    if (multiConstructor == null) {
      try {
        CellScanner cellScanner = this.rpcCall.getCellScanner();
        MutateResponse.Builder mutateResponseBuilder = MutateResponse.newBuilder();
        PayloadCarryingRpcController controller
                = new PayloadCarryingRpcController(cellScanner);
        addResultMutateResult(mutateResponseBuilder, null, controller);
        mutateResponseBuilder.setProcessed(true);
        Message result = mutateResponseBuilder.build();
        // we only update metrics while on Success
        ((RpcServer)this.callRunner.getRpcServer())
                .updateCallMetrics(startTime, result, (RpcServer.Call)rpcCall);
        doResponse(result, cellScanner, null);
      } catch (IOException e) {
        errorThrowable = e;
        doResponse(null, null, StringUtils.stringifyException(e));
      } finally {
        this.callRunner.getRpcServer().addCallSize(this.rpcCall.getSize() * -1);
        this.callRunner.cleanup();
        this.cleanup();
      }
    } else {
      try {
        multiConstructor.finishMutateAndTryResponse(retCodeDetails, mutationActions, hregion,
                startTime, batchContainsPuts, batchContainsDelete, errorThrowable, regionActionIndex);
      } catch (Exception e) {
        LOG.warn("multiTask.finishMutateAndTryResponse Exception:", e);
      } finally {
        this.cleanup();
      }
    }
  }

  protected void doResponse(Message result,
                  CellScanner cellScanner, String err) throws IOException {
    if (!this.rpcCall.isDelayed() || !this.rpcCall.isReturnValueDelayed()) {
      this.rpcCall.setResponse(result, cellScanner, errorThrowable, err);
    }
    this.rpcCall.sendResponseIfReady();
  }

  private void addResultMutateResult(final ClientProtos.MutateResponse.Builder builder, final Result result,
                         final PayloadCarryingRpcController rpcc) {
    if (result == null) return;
    if (rpcCall != null && rpcCall.isClientCellBlockSupport()) {
      builder.setResult(ProtobufUtil.toResultNoData(result));
      rpcc.setCellScanner(result.cellScanner());
    } else {
      ClientProtos.Result pbr = ProtobufUtil.toResult(result);
      builder.setResult(pbr);
    }
  }

  protected void cleanup() {
    this.multiConstructor = null;
    this.callRunner = null;
    this.mutateRequest = null;
    this.rpcCall = null;
    this.mutations = null;
    this.wal = null;
    this.errorThrowable = null;
    if (this.mutationActions != null) {
      this.mutationActions = null;
    }
  }

  private Mutation[] getmArray (RpcCall call,
                                List<Action> mutations) throws IOException {
    Mutation[] mArray = new Mutation[mutations.size()];
    CellScanner cells = call.getCellScanner();
//    long before = EnvironmentEdgeManager.currentTime();
    try {
      int i = 0;
      for (ClientProtos.Action action: mutations) {
        MutationProto m = action.getMutation();
        Mutation mutation;
        if (m.getMutateType() == MutationType.PUT) {
          mutation = ProtobufUtil.toPut(m, cells);
          this.batchContainsPuts = true;
        } else {
          mutation = ProtobufUtil.toDelete(m, cells);
          this.batchContainsDelete = true;
        }
        mArray[i++] = mutation;
//        quota.addMutation(mutation); TODO add quota, etc
      }
    } catch (Exception e) {
      LOG.warn("Exception while getting Mutation Array", e);
      throw e;
    }
    return mArray;
  }

  @Override
  public String getRegionName() {
    return hregion.getRegionNameAsString();
  }

}
