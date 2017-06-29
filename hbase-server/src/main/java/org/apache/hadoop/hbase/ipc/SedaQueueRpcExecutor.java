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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;

import com.google.protobuf.Message;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;

public class SedaQueueRpcExecutor extends BalancedQueueRpcExecutor {
  private static final Log LOG = LogFactory.getLog(SedaQueueRpcExecutor.class);

  private DefaultSedaStageManager stageManager = null;
  private boolean enableSedaMultiGet = true;

  public SedaQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final int maxQueueLength, final Configuration conf, final Abortable abortable) {
    super(name, handlerCount, numQueues, conf, abortable, LinkedBlockingQueue.class, maxQueueLength);
    stageManager = new DefaultSedaStageManager();
    stageManager.setConf(conf);
    DefaultSedaStage preAppendStage = new DefaultSedaStage(Stage.PreAppendStage);
    stageManager.register(preAppendStage);
    DefaultSedaStage finishSyncStage = new DefaultSedaStage(Stage.FinishSyncStage);
    stageManager.register(finishSyncStage);
    DefaultSedaStage finishAsyncStage = new DefaultSedaStage(Stage.FinishAsyncStage);
    stageManager.register(finishAsyncStage);
    MultiGetSedaStage multiGetStage = new MultiGetSedaStage(Stage.MultiGetStage);
    stageManager.register(multiGetStage);
    enableSedaMultiGet = conf.getBoolean("hbase.seda.multiget.enable", true);
    LOG.info("Use SedaQueueRpcExecutor.");
  }

  public void start(final int port) {
    super.start(port);
    stageManager.start();
  }

  public void stop() {
    super.stop();
    stageManager.shutdown();
  }

  @Override
  public boolean dispatch(CallRunner callTask) throws InterruptedException {
    RpcCall call = callTask.getRpcCall();
    ((RpcServer)callTask.getRpcServer()).CurCall.set(call);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Dispatch call=" + call);
    }
    Message param = call.getParam();
    if (isMutateRequest(param)) {
      MutateRequest mutate = (MutateRequest) param;
      MutationProto mutation = mutate.getMutation();
      MutationType type = mutation.getMutateType();
        if (type == MutationType.PUT || type == MutationType.DELETE) {
          if (!mutate.hasCondition()) {
            callTask.getRpcServer().getRsRpcServices().requestCount.increment();
            callTask.getRpcServer().getRsRpcServices().rpcMutateRequestCount.increment();
            MutatePreAppendTask mutatePreAppendTask
                    = new MutatePreAppendTask(callTask, mutate, mutation, type);
            if (mutatePreAppendTask.getMutations() != null)
              stageManager.getStage(Stage.PreAppendStage).add(mutatePreAppendTask);
            ((RpcServer)callTask.getRpcServer()).CurCall.set(null);
            return true;
          }
        }
    } else if (isMultiRequest(param)) {
      callTask.getRpcServer().getRsRpcServices().rpcMultiRequestCount.increment();
      MultiRequest multi = (MultiRequest) param;
      boolean allNonAtomicPutOrDelete = true;
      int actionCount = 0;
      int getCount = 0;
      for (RegionAction regionAction : multi.getRegionActionList()) {
        callTask.getRpcServer().getRsRpcServices().requestCount.add(regionAction.getActionCount());
        if (regionAction.hasAtomic() && regionAction.getAtomic()) {
          actionCount++;
          allNonAtomicPutOrDelete = false;
        } else {
          for (ClientProtos.Action action : regionAction.getActionList()) {
            actionCount++;
            if (action.hasGet()) {
              allNonAtomicPutOrDelete = false;
              getCount++;
            } else if (action.hasServiceCall()) {
              allNonAtomicPutOrDelete = false;
            } else if (action.hasMutation()) {
              MutationType type = action.getMutation().getMutateType();
              switch (type) {
              case APPEND:
                allNonAtomicPutOrDelete = false;
                break;
              case INCREMENT:
                allNonAtomicPutOrDelete = false;
                break;
              case PUT:
              case DELETE:
                break;
              default:
                allNonAtomicPutOrDelete = false;
              }
            }
          }
        }
      }

      if (allNonAtomicPutOrDelete) {
        MultiMutateResponseConstructor resultConstructor =
            new MultiMutateResponseConstructor(callTask, (HRegionServer) abortable, multi.getRegionActionCount());
        // MultiRequest contains only non atomic put or delete, split it to SEDA process.
        int regionActionIndex = -1;
        for (RegionAction regionAction : multi.getRegionActionList()) {
          if (regionAction.getActionList() == null) {
            LOG.info("regionAction.getActionList() == null:" + regionAction.toString());
          } else if (regionAction.getActionList().isEmpty()) {
            LOG.info("regionAction.getActionList().isEmpty():" + regionAction.toString());
          }
          // Finish up any outstanding mutations
          if (regionAction.getActionList() != null && !regionAction.getActionList().isEmpty()) {
            Region hregion;
            regionActionIndex++;
            try {
              hregion = callTask.getRpcServer()
                      .getRsRpcServices().getRegion(regionAction.getRegion());
//                quota = getQuotaManager().checkQuota(region, regionAction.getActionList());
            } catch (IOException e) {
              callTask.getRpcServer().getMetrics().exception(e);
              resultConstructor.setExceptionMutateAndTryResponse(e, regionAction, regionActionIndex);
              continue;  // For this region it's a failure.
            }
            try {
              MutatePreAppendTask mutatePreAppendTask
                      = new MutatePreAppendTask(resultConstructor, hregion,
                      regionAction.getActionList(), call, regionActionIndex);
              if (mutatePreAppendTask.getMutations() != null
                      && mutatePreAppendTask.getMutations().length > 0)
                // TODO reclaimMemStoreMemory
//                if (!hregion.getRegionInfo().isMetaTable()) {
//                  callTask.getRpcServer().getRsRpcServices().regionServer.cacheFlusher.reclaimMemStoreMemory();
//                }
                stageManager.getStage(Stage.PreAppendStage).add(mutatePreAppendTask);
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
        if(multi.getRegionActionCount() - 1 != regionActionIndex) {
          LOG.warn("RegionActionCount mismatched! RegionActionCount is:"
                  + multi.getRegionActionCount() + "; regionActionIndex is:"
                  + regionActionIndex + "; multi.getRegionActionList().size is:"
                  + multi.getRegionActionList().size());
        }
        ((RpcServer)callTask.getRpcServer()).CurCall.set(null);
        return true;
      }
      if (enableSedaMultiGet && getCount == actionCount && getCount > 1) {
        // MultiRequest contains only get, split into sub get tasks and dispatch tasks for
        // concurrent process.
        MultiGetResponseConstructor resultConstructor =
            new MultiGetResponseConstructor(callTask, getCount, multi.getRegionActionList().size());
        int regionIndex = 0;
        for (RegionAction regionAction : multi.getRegionActionList()) {
          resultConstructor.allocate(regionIndex, regionAction.getActionCount());
          int actionIndex = 0;
          for (ClientProtos.Action action : regionAction.getActionList()) {
            stageManager.getStage(Stage.MultiGetStage).add(
              new SubGetTask(callTask, resultConstructor, regionAction, action, regionIndex,
                  actionIndex));
            actionIndex++;
          }
          regionIndex++;
        }
        ((RpcServer) callTask.getRpcServer()).CurCall.set(null);
        return true;
      }
    }
    ((RpcServer)callTask.getRpcServer()).CurCall.set(null);
    // dispatch to old process.
    return super.dispatch(callTask);
  }

  private boolean isMultiRequest(final Message param) {
    if (param instanceof MultiRequest) {
      return true;
    }
    return false;
  }

  private boolean isMutateRequest(final Message param) {
    if (param instanceof MutateRequest) {
      return true;
    }
    return false;
  }
}
