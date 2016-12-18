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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import com.google.protobuf.Message;

/**
 * Facility class to return a {@link Future} instance for non-blocking call
 * @param <V> The result type returned by this Future's {@code get} method
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AsyncFuture<V> implements Future<V>{
  HTable table;
  int maxAttempts = 1;// at least try once
  AtomicInteger numAttempts = new AtomicInteger(0);
  // by means limit parallel retry threads to 100 to avoid too much pressure to server
  static final ScheduledThreadPoolExecutor retryExecutor = new ScheduledThreadPoolExecutor(100);
  volatile boolean isCanceled = false;
  protected Collection<Future<?>> retryFutures =
      Collections.synchronizedCollection(new ArrayList<Future<?>>());

  /**
   * Waits if necessary for the request to complete, and then retrieves its result.
   * @return the requested result
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws ExecutionException if any error occurs during the process
   */
  @Override
  public abstract V get() throws InterruptedException, ExecutionException;

  /**
   * Waits if necessary for at most the given time for the request to complete, and then retrieves
   * its result, if available.
   * @param timeout the maximum time to wait
   * @param unit the time unit of the timeout argument
   * @return the requested result
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws ExecutionException if any error occurs during the process
   * @throws TimeoutException if the wait timed out
   */
  public abstract V get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException;

  /**
   * Retry in another thread to avoid blocking the netty pipeline, in a delayed way
   * @param actions The actions to retry
   * @param pause The pause before retry
   * @param callback The callback to notify on failure
   */
  protected void delayedRetry(final List<Action<Row>> actions, long pause,
      final AsyncRpcCallback<? extends Message> callback) {
    if (isCanceled) {
      return;
    }

    Future<?> retryFuture = retryExecutor.schedule(new Runnable() {

      @Override
      public void run() {
        doRequest(actions, callback);
      }
    }, pause, TimeUnit.MILLISECONDS);
    synchronized (retryFutures) {
      retryFutures.add(retryFuture);
    }
  }

  /**
   * Method to issue request
   * @param actions The actions to request
   * @param callback The callback to run
   */
  abstract void doRequest(List<Action<Row>> actions, AsyncRpcCallback<? extends Message> callback);

  /**
   * Currently we set the isCanceled flag and quit further retry to avoid retry thread pool is
   * occupied by stale futures
   */
  public boolean cancel(boolean mayInterruptIfRunning) {
    isCanceled = true;
    boolean couldCancel = true;
    synchronized (retryFutures) {
      if (!retryFutures.isEmpty()) {
        for (Future<?> retryFuture : retryFutures) {
          couldCancel = (retryFuture.cancel(mayInterruptIfRunning) && couldCancel);
        }
        retryFutures.clear();
      }
    }
    return couldCancel;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isCancelled() {
    return isCanceled;
  }
}
