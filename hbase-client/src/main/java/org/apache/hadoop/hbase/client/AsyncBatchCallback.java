/**
 *
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

/**
 * Facility class for easily customizing a batch callback, for async calls like asyncBatchGets,
 * asyncBatchPuts,asyncBatchDeletes.
 * <p/>
 * The batch operation will be grouped by regionserver, so the callback is also called at group
 * level. It only processes the callback of whole batch when all groups finish.
 * <p/>
 * There're two possible cases of failure: 1. the whole batch fails or 2. part of the batch fails;
 * the 1st case is handled in {@link #onError(Throwable)} or {@link #processError(Throwable, List)} and
 * the 2nd should be handled in {@link #processSingleResult(MultiAction, MultiResponse, ServerName)}
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AsyncBatchCallback implements AsyncRpcCallback<ClientProtos.MultiResponse> {
  private static final Log LOG = LogFactory.getLog(AsyncBatchCallback.class);

  protected final Object[] results;
  private final AtomicLong actionsInProgress; // tracking the overall progress
  private List<Action<Row>> actions; // saved to retry the whole batch

  /**
   * used in callback wrapper to save MultiAction, @see {@link AsyncBatchCallbackWrapper}
   */
  protected MultiAction<Row> multiAction;
  /**
   * used in callback wrapper to save ServerName, @see {@link AsyncBatchCallbackWrapper}
   */
  protected ServerName server;

  /**
   * @return current unfinished actions
   */
  public AtomicLong getActionsInProgress() {
    return actionsInProgress;
  }

  /**
   * Set actions for this batch
   * @param actions the actions for this batch
   */
  public void setActions(List<Action<Row>> actions) {
    this.actions = actions;
  }

  /**
   * @return actions issued for this callback
   */
  public List<Action<Row>> getActions() {
    return actions;
  }

  /**
   * Constructor for customized callback
   * @param results array to store the batch result
   */
  public AsyncBatchCallback(Object[] results) {
    this.results = results;
    this.actionsInProgress = new AtomicLong(results.length);
  }

  /**
   * Constructor for customized callback
   * @param results array to store the batch result
   * @param actions the batch actions to run
   */
  public AsyncBatchCallback(Object[] results, List<Action<Row>> actions) {
    this.results = results;
    this.actionsInProgress = new AtomicLong(results.length);
    this.actions = actions;
  }

  /**
   * Constructor for retry
   * @param results array to store the batch result
   * @param actionsInProgress the counter to track actions in progress
   * @param toReplay the batch actions to replay
   */
  public AsyncBatchCallback(Object[] results, AtomicLong actionsInProgress,
      List<Action<Row>> toReplay) {
    this.results = results;
    this.actionsInProgress = actionsInProgress;
    this.actions = toReplay;
  }

  /**
   * Constructor for callback wrapper
   * @param results array to store the batch result
   * @param actionsInProgress the counter to track actions in progress
   * @param actions the batch actions to run
   * @param multiAction {@link MultiAction} instance to save
   * @param server {@link ServerName} instance to save
   */
  public AsyncBatchCallback(Object[] results, AtomicLong actionsInProgress,
      List<Action<Row>> actions, MultiAction<Row> multiAction, ServerName server) {
    this.results = results;
    this.actionsInProgress = actionsInProgress;
    this.actions = actions;
    this.multiAction = multiAction;
    this.server = server;
  }

  /**
   * Called when one group action of the batch operation completes
   * @param request the request for batch callback
   * @param result the corresponding result of given call
   */
  public void onComplete(ClientProtos.MultiRequest request, ClientProtos.MultiResponse result,
      CellScanner cellScanner) {
    if ((request instanceof ClientProtos.MultiRequest)
        && (result instanceof ClientProtos.MultiResponse)) {
      MultiResponse response;
      try {
        response =
            ResponseConverter.getResults((ClientProtos.MultiRequest) request,
              (ClientProtos.MultiResponse) result, cellScanner);
      } catch (IOException e) {
        processError(e, this.actions);
        return;
      }
      processSingleResult(this.multiAction, response, this.server);
    } else {
      processError(new DoNotRetryIOException(
          "Expecting request/response type to be <MultiRequest,MultiResponse> but got: <"
              + request.getClass().getName() + "," + result.getClass().getName()
              + ">, please check and use correct type of callback"), null);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Left action count: " + actionsInProgress);
    }
    if (actionsInProgress.get() == 0) {
      processBatchResult(results);
    }
  }

  @Override
  public void run(ClientProtos.MultiResponse result) {
    // dummy method, we don't use this for batch
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onError(Throwable exception) {
    processError(exception, this.actions);
  }

  @Override
  public void setLocation(HRegionLocation location) {
    // dummy method, we don't use this for batch
  }

  /**
   * Called when a single group actions of the batch operation completes successfully
   * @param multiAction the orginized MultiAction instance
   * @param result the result to process
   * @param server from which server this result is returned
   */
  public abstract void processSingleResult(MultiAction<Row> multiAction, MultiResponse result,
      ServerName server);

  /**
   * Called when the whole batch completes
   * @param results the array containing results of the whole batch
   */
  public abstract void processBatchResult(Object[] results);

  /**
   * Called when a single group actions of the batch operation fails with exception
   * @param exception the thrown exception for the batch
   * @param toRetry the action list to retry
   */
  public abstract void processError(Throwable exception, List<Action<Row>> toRetry);
}
