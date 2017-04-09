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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ExecutionList;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.RetriesExhaustedException.ThrowableWithExtraContext;

import com.google.protobuf.Message;

/**
 * Facility class to return a {@link Future} instance for non-blocking call
 * @param <V> The result type returned by this Future's {@code get} method
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AsyncFuture<V> implements Future<V> {
  protected HTable table;
  protected int maxAttempts = 1;// at least try once
  protected AtomicInteger numAttempts = new AtomicInteger(0);
  protected IOException toThrow; // exception to throw
  protected V toReturn; // result to return
  // tracking exceptions during retry
  // must be thread safe since this list might be updated in different callbacks
  protected List<ThrowableWithExtraContext> exceptions = Collections
      .synchronizedList(new ArrayList<RetriesExhaustedException.ThrowableWithExtraContext>());
  protected volatile boolean isCanceled = false;
  protected volatile boolean isDone = false;
  protected final long startTime = System.currentTimeMillis();
  protected volatile long executionTime = -1;
  protected ExecutionList executionList = new ExecutionList();

  // set core pool size to 10 to avoid too many idle threads
  private static final ScheduledThreadPoolExecutor retryExecutor =
      new ScheduledThreadPoolExecutor(10);
  private Collection<Future<?>> retryFutures =
      Collections.synchronizedCollection(new ArrayList<Future<?>>());

  public long getExecutionTime() throws IOException {
    if (executionTime < 0)
      throw new IOException("future is not done, executionTime is invalid.");
    return executionTime;
  }

  /**
   * Mark the future's status as done.
   */
  protected abstract void markDone();

  /**
   * Waits if necessary for the request to complete, and then retrieves its result. The maximum time
   * to wait follows the {@link HConstants#HBASE_CLIENT_OPERATION_TIMEOUT} setting, in milliseconds.
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

  /**
   * Check whether this future is already canceled or done
   * @return true if the future is already done with a result, false if the future is neither
   *         canceled or done
   * @throws ExecutionException if the future is already canceled
   */
  public boolean checkCancelOrDone() throws ExecutionException {
    if (isCanceled) {
      throw new ExecutionException(new DoNotRetryIOException("This future is already canceled"));
    }
    if (isDone) {
      if (toThrow != null) {
        throw new ExecutionException(toThrow);
      }
      return true;
    }
    return false;
  }

  /**
   * Registers a listener to be {@linkplain Executor#execute(Runnable) run} on the given executor.
   * The listener will run when the {@code Future}'s computation is {@linkplain AsyncFuture#isDone()
   * complete} or, if the computation is already complete, immediately.
   *
   * @param listener The listener to invoke when {@code future} is completed.
   * @param executor The executor to run {@code listener} when the future completes.
   */
  public AsyncFuture addListener(final Listener<V> listener, Executor executor) {
    Preconditions.checkNotNull(listener, "Listener was null.");
    Preconditions.checkNotNull(executor, "Executor was null.");

    synchronized (executionList) {
      executionList.add(new Runnable() {
        @Override
        public void run() {
          V val;
          try {
            val = get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            listener.onError(e);
            return;
          } catch (ExecutionException e) {
            e.printStackTrace();
            listener.onError(e.getCause());
            return;
          }
          listener.onComplete(val);
        }
      }, executor);

      if (isDone) {
        notifyListener();
      }
    }

    return this;
  }

  protected void notifyListener() {
    synchronized (executionList) {
      executionList.execute();
    }
  }
}
