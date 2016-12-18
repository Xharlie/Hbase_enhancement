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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.AsyncProcess.AsyncRequestFutureImpl;
import org.apache.hadoop.hbase.client.RetriesExhaustedException.ThrowableWithExtraContext;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Message;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncBatchFuture extends AsyncFuture<Object[]> {
  private static Log LOG = LogFactory.getLog(AsyncBatchFuture.class);

  final String tableName;
  final String firstRow;
  final CountDownLatch latch;
  final int startLogErrorsCnt;
  final int operationTimeout;

  public AsyncBatchFuture(final HTable table, final List<Action<Row>> actions,
      final Object[] results, int tries, final long retryPause, int startLogErrorsCnt,
      int operationTimeout) throws IOException {
    assert actions.size() == results.length : "Number of actions: " + actions.size()
        + " doesn't equal to length of result array: " + results.length;
    this.table = table;
    this.latch = new CountDownLatch(actions.size());
    this.firstRow = Bytes.toString(actions.get(0).getAction().getRow());
    this.toReturn = results;
    this.maxAttempts = tries;
    this.tableName = table.getName().getNameAsString();
    this.startLogErrorsCnt = startLogErrorsCnt;
    this.operationTimeout = operationTimeout;
    AsyncBatchCallback batchCallback = new AsyncBatchCallbackImpl(results, actions, retryPause);
    doRequest(actions, batchCallback);
  }

  @Override
  public Object[] get() throws InterruptedException, ExecutionException {
    if (checkCancelOrDone()) {
      return toReturn;
    }

    latch.await(operationTimeout, TimeUnit.MILLISECONDS);
    if (latch.getCount() > 0) {
      cancel(true);
      throw new InterruptedException("Failed to get batch result from table: " + this.tableName
          + " within operation timeout: " + operationTimeout + TimeUnit.MILLISECONDS);
    }
    if (toThrow != null) {
      throw new ExecutionException(toThrow);
    }
    if (!exceptions.isEmpty()) {
      RetriesExhaustedException cause =
          new RetriesExhaustedException(numAttempts.get() - 1, exceptions);
      throw new ExecutionException(cause);
    }
    return toReturn;
  }

  @Override
  void doRequest(List<Action<Row>> actions, AsyncRpcCallback<? extends Message> callback) {
    // Notice: we use an overall counter for retry attempt rather than per-action-group
    while (numAttempts.getAndIncrement() < maxAttempts) {
      try {
        this.table.groupAndSendMultiAction(actions, toReturn, (AsyncBatchCallback) callback);
        break;
      } catch (IOException e) {
        if (e instanceof DoNotRetryIOException) {
          callback.onError(e);
          break;
        }
        LOG.debug("Failed to issue async multi request for table " + this.tableName
            + "; numAttempts=" + numAttempts + ", maxAttempts=" + maxAttempts, e);
        if (numAttempts.get() == maxAttempts) {
          callback.onError(e);
          break; // break to save one compare
        }
      } catch (Exception e) {
        // unexpected exception such as ClassCastException, call onError directly
        callback.onError(e);
        break;
      }
    }
  }

  @Override
  public Object[] get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (checkCancelOrDone()) {
      return toReturn;
    }

    latch.await(timeout, unit);
    if (latch.getCount() > 0) {
      cancel(true);
      throw new TimeoutException("Failed to get batch result against table: "
          + this.tableName + " within " + timeout + " " + unit.name().toLowerCase());
    }
    if (toThrow != null) {
      throw new ExecutionException(toThrow);
    }
    if (!exceptions.isEmpty()) {
      RetriesExhaustedException cause =
          new RetriesExhaustedException(numAttempts.get() - 1, exceptions);
      throw new ExecutionException(cause);
    }
    return toReturn;
  }

  @Override
  public String toString() {
    return "Batch[table=" + tableName + ", first row=" + firstRow + "]";
  }

  class AsyncBatchCallbackImpl extends AsyncBatchCallback {
    final long retryPause;

    public AsyncBatchCallbackImpl(Object[] results, List<Action<Row>> actions, long retryPause) {
      super(results, actions);
      this.retryPause = retryPause;
    }

    public AsyncBatchCallbackImpl(Object[] results, AtomicLong actionsInProgress,
        List<Action<Row>> actions, long retryPause) {
      super(results, actionsInProgress, actions);
      this.retryPause = retryPause;
    }

    @Override
    public void processSingleResult(MultiAction<Row> multiAction, MultiResponse result, ServerName server) {
      // set result and exceptions and find out actions to retry
      List<Action<Row>> toReplay = receiveMultiAction(multiAction, server, result);

      // retry the failed parts
      if (!toReplay.isEmpty()) {
        processError(new IOException("There're still " + toReplay.size() + " actions failed"),
          toReplay);
      }
    }

    /**
     * Called when we receive the result of a server query.
     * <p/>
     * Main logic refers to receiveMultiAction method in {@link AsyncRequestFutureImpl}
     * @param multiAction - the multiAction we sent
     * @param server - the location. It's used as a server name.
     * @param responses - the response
     * @return the list of actions to retry
     */
    private List<Action<Row>> receiveMultiAction(MultiAction<Row> multiAction, ServerName server,
        MultiResponse responses) {
      assert responses != null;

      // Success or partial success
      // Analyze detailed results. We can still have individual failures to be redo.
      // two specific throwables are managed:
      // - DoNotRetryIOException: we continue to retry for other actions
      // - RegionMovedException: we update the cache with the new region location

      List<Action<Row>> toReplay = new ArrayList<Action<Row>>();
      Throwable throwable = null;

      // Go by original action.
      for (Map.Entry<byte[], List<Action<Row>>> regionEntry : multiAction.actions.entrySet()) {
        byte[] regionName = regionEntry.getKey();
        Map<Integer, Object> regionResults = responses.getResults().get(regionName);
        if (regionResults == null) {
          if (!responses.getExceptions().containsKey(regionName)) {
            LOG.error("Server sent us neither results nor exceptions for "
                + Bytes.toStringBinary(regionName));
            responses.getExceptions().put(regionName, new RuntimeException("Invalid response"));
          }
          continue;
        }
        boolean regionFailureRegistered = false;
        for (Action<Row> sentAction : regionEntry.getValue()) {
          Object result = regionResults.get(sentAction.getOriginalIndex());
          // Failure: retry if it's make sense else update the errors lists
          if (result == null || result instanceof Throwable) {
            Row row = sentAction.getAction();
            throwable = ClientExceptionsUtil.findException(result);
            // Register corresponding failures once per server/once per region.
            if (!regionFailureRegistered) {
              regionFailureRegistered = true;
              table.connection.updateCachedLocations(table.getName(), regionName, row.getRow(),
                result, server);
            }
            // we set error anyway, will get overwritten if succeed in retry
            // notice that we don't support replica read here
            results[sentAction.getOriginalIndex()] = throwable;
            toReplay.add(sentAction);
          } else {
            // update the stats about the region, if its a user table. We don't want to slow down
            // updates to meta tables, especially from internal updates (master, etc).
            if (table.connection.getStatisticsTracker() != null) {
              result =
                  ResultStatsUtil.updateStats(result, table.connection.getStatisticsTracker(),
                    server, regionName);
            }
            // set result
            results[sentAction.getOriginalIndex()] = result;
            latch.countDown();
            getActionsInProgress().decrementAndGet();
          }
        }
      }

      // The failures global to a region.
      // Use multiAction we sent previously to find the actions to replay.
      for (Map.Entry<byte[], Throwable> throwableEntry : responses.getExceptions().entrySet()) {
        throwable = throwableEntry.getValue();
        byte[] region = throwableEntry.getKey();
        List<Action<Row>> actions = multiAction.actions.get(region);
        if (actions == null || actions.isEmpty()) {
          throw new IllegalStateException("Wrong response for the region: "
              + HRegionInfo.encodeRegionName(region));
        }
        table.connection.updateCachedLocations(table.getName(), region, actions.get(0).getAction()
            .getRow(), throwable, server);

        for (Action<Row> action : actions) {
          // we set error anyway, will get overwritten if succeed in retry
          // notice that we don't support replica read here
          results[action.getOriginalIndex()] = throwable;
          toReplay.add(action);
        }
      }
      return toReplay;
    }

    @Override
    public void processError(Throwable exception, List<Action<Row>> toRetry) {
      if (exception instanceof DoNotRetryIOException) {
        if (exceptions.isEmpty()) {
          toThrow = (DoNotRetryIOException) exception;
        } else {
          exceptions
              .add(new ThrowableWithExtraContext(exception, System.currentTimeMillis(), null));
        }
        for (int i = 0; i < getActions().size(); i++) {
          latch.countDown();
          getActionsInProgress().decrementAndGet();
        }
        return;
      }
      // add exception to exception list
      ThrowableWithExtraContext exceptionWithDetails =
          new ThrowableWithExtraContext(exception, System.currentTimeMillis(), null);
      exceptions.add(exceptionWithDetails);
      // check and retry
      int attempts = numAttempts.get();
      if (attempts < maxAttempts && !isCanceled) {
        if (attempts > startLogErrorsCnt) {
          LOG.warn("Multi attempt failed for table " + tableName + "; tried=" + attempts
              + ", maxAttempts=" + maxAttempts + ". Retry...", exception);
        }
        long pause = ConnectionUtils.getPauseTime(retryPause, attempts);
        AsyncBatchCallbackImpl callback =
            new AsyncBatchCallbackImpl(results, getActionsInProgress(), toRetry, retryPause);
        delayedRetry(toRetry, pause, callback);
      } else {
        // FIXME should add logic to set the exception as result of the relative request in result
        // array, refer to AsyncProcess$AsyncRequestFutureImpl#setError
        isDone = true;
        if (isCanceled) {
          toThrow = new DoNotRetryIOException("Request is already canceled");
        } else {
          toThrow = new RetriesExhaustedException(attempts - 1, exceptions);
        }
        for (int i = 0; i < toRetry.size(); i++) {
          latch.countDown();
          getActionsInProgress().decrementAndGet();
        }
      }
    }

    @Override
    public void processBatchResult(Object[] results) {
      isDone = true;
      if (LOG.isTraceEnabled()) LOG.trace("Batch completed");
    }

  }

  @Override
  public boolean isDone() {
    return isDone;
  }
}
