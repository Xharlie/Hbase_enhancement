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
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Message;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AsyncGetFuture extends AsyncFuture<Result> {
  private static Log LOG = LogFactory.getLog(AsyncGetFuture.class);

  final Get get;
  final String row;
  final String tableName;
  Result toReturn;
  ArrayList<ThrowableWithExtraContext> exceptions = new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>();
  IOException toThrow = null;
  CountDownLatch latch = new CountDownLatch(1);
  boolean isDone = false;

  public AsyncGetFuture(final HTable table, final Get get, int tries, final long retryPause,
      final int startLogErrorsCnt) throws IOException {
    this.table = table;
    this.get = get;
    this.maxAttempts = tries;
    this.row = Bytes.toString(get.getRow());
    this.tableName = table.getName().getNameAsString();
    AsyncGetCallback callback = new AsyncGetCallback(this.get.getRow()) {

      @Override
      public void processResult(Result result) {
        toReturn = result;
        isDone = true;
        latch.countDown();
      }

      @Override
      public void processError(Throwable exception) {
        if (exception instanceof DoNotRetryIOException) {
          toThrow = (DoNotRetryIOException) exception;
          isDone = true;
          latch.countDown();
          return;
        }
        // add exception to the exception list
        ThrowableWithExtraContext exceptionWithDetails =
            new ThrowableWithExtraContext(exception, System.currentTimeMillis(), null);
        exceptions.add(exceptionWithDetails);
        // check and retry
        int attempts = numAttempts.get();
        if (attempts < maxAttempts) {
          if (attempts > startLogErrorsCnt) {
            LOG.debug("Get attempt failed for table " + tableName + " on row " + row + "; tried="
                + attempts + ", maxAttempts=" + maxAttempts + ". Retry...",
              exception);
          }
          long pause = ConnectionUtils.getPauseTime(retryPause, attempts);
          // update cached location if necessary
          table.connection.updateCachedLocations(table.getName(),
            location.getRegionInfo().getRegionName(), get.getRow(), exception,
            location.getServerName());
          delayedRetry(null, pause, this);
        } else {
          toThrow = new RetriesExhaustedException(attempts - 1, exceptions);
          isDone = true;
          latch.countDown();
        }
      }
    };
    doRequest(null, callback);
  }

  @Override
  public Result get() throws InterruptedException, ExecutionException {
    latch.await();
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
          this.table.asyncGet(get, (AsyncRpcCallback<GetResponse>) callback);
          break;
        } catch (ClassCastException e) {
          throw new DoNotRetryIOException("Type of given callback is not correct");
        }
      } catch (IOException e) {
        LOG.debug("Failed to issue async get request for table " + this.tableName + " on row "
            + this.row + "; numAttempts=" + numAttempts + ", maxAttempts=" + maxAttempts,
          e);
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
    latch.await(timeout, unit);
    if (latch.getCount() > 0) {
      throw new TimeoutException("Failed to get row: " + this.row + " from table: "
          + this.tableName + " within " + timeout + " " + unit.name().toLowerCase());
    }
    if (toThrow != null) {
      throw new ExecutionException(toThrow);
    }
    return toReturn;
  }

  @Override
  public String toString() {
    return "Get[table=" + tableName + ",row=" + row + "]";
  }

  @Override
  public boolean isDone() {
    return isDone;
  }

}
