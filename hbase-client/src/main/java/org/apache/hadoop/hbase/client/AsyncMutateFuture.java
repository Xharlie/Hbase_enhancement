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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.RetriesExhaustedException.ThrowableWithExtraContext;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Message;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncMutateFuture extends AsyncFuture<Result> {
  private static Log LOG = LogFactory.getLog(AsyncMutateFuture.class);

  final Mutation mutate;
  final String row;
  final String tableName;
  final int operationTimeout;
  final AsyncMutateCallback callback;
  CountDownLatch latch = new CountDownLatch(1);

  public AsyncMutateFuture(final HTable table, final Mutation mutate, int tries,
      final long retryPause, final int startLogErrorsCnt, int operationTimeout) throws IOException {
    this.table = table;
    this.mutate = mutate;
    this.maxAttempts = tries;
    this.row = Bytes.toString(mutate.getRow());
    this.tableName = table.getName().getNameAsString();
    this.operationTimeout = operationTimeout;
    this.callback = new AsyncMutateCallback(this.mutate.getRow()) {

      @Override
      public void processResult(Result result) {
        toReturn = result;
        markDone();
      }

      @Override
      public void processError(Throwable exception) {
        if (exception instanceof DoNotRetryIOException) {
          toThrow = (DoNotRetryIOException) exception;
          markDone();
          return;
        }
        // add exception to the exception list
        ThrowableWithExtraContext exceptionWithDetails =
            new ThrowableWithExtraContext(exception, System.currentTimeMillis(), null);
        exceptions.add(exceptionWithDetails);
        // check and retry
        int attempts = numAttempts.get();
        if (attempts < maxAttempts && !isCanceled) {
          if (attempts > startLogErrorsCnt) {
            LOG.debug(
              "Mutate attempt failed for table " + tableName + " on row " + row + " at {" + location
                  + "}; tried=" + attempts + ", maxAttempts=" + maxAttempts + ". Retry...",
              exception);
          }
          long pause = ConnectionUtils.getPauseTime(retryPause, attempts);
          // update cached location if necessary
          table.connection.updateCachedLocations(table.getName(),
            location.getRegionInfo().getRegionName(), mutate.getRow(), exception,
            location.getServerName());
          delayedRetry(null, pause, this);
        } else {
          LOG.debug("Mutate failed after tried " + numAttempts + " times", exception);
          if (isCanceled) {
            toThrow = new DoNotRetryIOException("Request is already canceled");
          } else {
            toThrow = new RetriesExhaustedException(attempts - 1, exceptions);
          }
          markDone();
        }
      }
    };
    doRequest(null, callback);
  }

  @Override
  protected void markDone(){
    executionTime = System.currentTimeMillis() - startTime;
    isDone = true;
    latch.countDown();
    notifyListener();
  }

  @Override
  public Result get() throws InterruptedException, ExecutionException {
    if (checkCancelOrDone()) {
      return toReturn;
    }

    latch.await(this.operationTimeout, TimeUnit.MILLISECONDS);
    if (latch.getCount() > 0) {
      cancel(true);
      throw new InterruptedException("Failed to put row: " + this.row + " into table: "
          + this.tableName + " at {" + this.callback.location + "} within operation timeout: "
          + this.operationTimeout + TimeUnit.MILLISECONDS);
    }
    if (toThrow != null) {
      throw new ExecutionException(toThrow);
    }
    return toReturn;
  }

  @SuppressWarnings("unchecked")
  @Override
  void doRequest(List<Action<Row>> actions, AsyncRpcCallback<? extends Message> callback) {
    while (numAttempts.getAndIncrement() < maxAttempts) {
      try {
        try {
          this.table.asyncMutate(mutate, (AsyncRpcCallback<MutateResponse>) callback);
          break;
        } catch (ClassCastException e) {
          throw new DoNotRetryIOException("Type of given callback is not correct");
        }
      } catch (IOException e) {
        if (e instanceof DoNotRetryIOException) {
          callback.onError(e);
          break;
        }
        LOG.debug("Failed to issue async mutate request for table " + tableName + " on row " + row
            + " at {" + this.callback.location + "}; numAttempts=" + numAttempts + ", maxAttempts="
            + maxAttempts,e);
        if (numAttempts.get() == maxAttempts) {
          callback.onError(e);
          break; // break to save one compare
        }
      }
    }
  }

  @Override
  public Result get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (checkCancelOrDone()) {
      return toReturn;
    }

    latch.await(timeout, unit);
    if (latch.getCount() > 0) {
      throw new TimeoutException("Failed to put row: " + this.row + " into table: "
          + this.tableName + " within " + timeout + " " + unit.name().toLowerCase());
    }
    if (toThrow != null) {
      throw new ExecutionException(toThrow);
    }
    return toReturn;
  }

  @Override
  public String toString() {
    return "Mutate[table=" + tableName + ",row=" + row + "]";
  }

  @Override
  public boolean isDone() {
    return isDone;
  }

}
