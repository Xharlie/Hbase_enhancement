/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.protobuf.Message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Class to test Async Call Method of HTable.Spins up the minicluster once at
 * test start and then takes it down afterward.
 */
@Category(LargeTests.class)
public class TestAsyncHTable {
  @Rule public TestName name = new TestName();
  final Log LOG = LogFactory.getLog(getClass());
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
  private final byte[] qualifier = Bytes.toBytes("q");
  private final int threadsNum = 5;
  private final int operationPerThread = 100;
  private int expectedOperationCount = threadsNum * operationPerThread;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Connection connection;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_PAUSE, 100);
    // TEST_UTIL.getConfiguration().setInt("hbase.ipc.server.max.callqueue.length", 12800);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
    // TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 5000);
    // comment below line to suppress retry log
    TEST_UTIL.getConfiguration().setInt(AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    if (name.getMethodName().equals("testWithBlockingClient")) {
      TEST_UTIL.getConfiguration().set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
        RpcClientImpl.class.getName());
    } else {
      TEST_UTIL.getConfiguration().set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
        AsyncRpcClient.class.getName());
    }
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @After
  public void tearDown() throws Exception {
    connection.close();
  }

  @Test(timeout = 60000)
  public void testBatch() throws Exception {
    // Use a customized callback for put, and AsyncFuture for get
    // TODO use multiple thread to check error handling like call queue too big
    final byte[] table = Bytes.toBytes(name.getMethodName());
    final byte[] qualifier = Bytes.toBytes("q");
    HTable htable = TEST_UTIL.createTable(table, COLUMN_FAMILY,
      new byte[][] { Bytes.toBytes("thread2"), Bytes.toBytes("thread4") });
    int batchSize = 10;
    final AtomicInteger successCount = new AtomicInteger(0);
    final AtomicInteger errorCount = new AtomicInteger(0);

    final class CustomAsyncBatchCallback extends AsyncBatchCallback {

      public CustomAsyncBatchCallback(Object[] results) {
        super(results);
      }

      @Override
      public void processSingleResult(MultiAction<Row> multiAction, MultiResponse result,
          ServerName server) {
        LOG.debug("MultiAction: " + multiAction.actions);
        for (Map<Integer, Object> res : result.getResults().values()) {
          // LOG.debug("Results: " + res.values());
          for (Map.Entry<Integer, Object> entry : res.entrySet()) {
            int index = entry.getKey();
            Object obj = entry.getValue();
            results[index] = obj;
            if (obj instanceof Throwable) {
              errorCount.incrementAndGet();
            } else {
              LOG.debug(
                "Result: " + Bytes.toString(((Result) obj).getValue(COLUMN_FAMILY, qualifier)));
              successCount.incrementAndGet();
              getActionsInProgress().decrementAndGet();
            }
          }
        }
        for (Throwable e : result.getExceptions().values()) {
          LOG.debug("Exception: ", e);
          errorCount.incrementAndGet();
        }
      }

      @Override
      public void processBatchResult(Object[] results) {
        LOG.debug("Batch complete!");
      }

      @Override
      public void processError(Throwable exception, List<Action<Row>> toRetry) {
        LOG.debug("One group of actions failed!", exception);
      }

    }

    List<Row> puts = new ArrayList<Row>();
    for (int i = 0; i < batchSize; i++) {
      byte[] row = Bytes.toBytes("thread" + i);
      Put put = new Put(row);
      put.addColumn(COLUMN_FAMILY, qualifier, row);
      puts.add(put);
    }
    Object[] results = new Object[puts.size()];
    htable.asyncBatch(puts, results, new CustomAsyncBatchCallback(results));

    waitUntilEqual(successCount, errorCount, batchSize, 60 * 1000);
    LOG.debug("Succeed put operation number: " + successCount.get());
    LOG.debug("Failed put operation number: " + errorCount.get());
    assertEquals(successCount.get(), TEST_UTIL.countRows(htable));

    // Reset counters
    successCount.set(0);
    errorCount.set(0);

    List<Row> gets = new ArrayList<Row>();
    for (int i = 0; i < batchSize; i++) {
      byte[] row = Bytes.toBytes("thread" + i);
      Get get = new Get(row);
      gets.add(get);
    }
    results = new Object[gets.size()];
    AsyncFuture<Object[]> future = htable.asyncBatch(gets, results);
    // get result and make sure the result array got from future is the original one we passed in
    assert results == future.get();
    for (Object result : results) {
      if (result == null || result instanceof Throwable) {
        LOG.debug("Received one error: " + result);
        errorCount.incrementAndGet();
      } else {
        LOG.debug("Received one result: "
            + Bytes.toString(((Result) result).getValue(COLUMN_FAMILY, qualifier)));
        successCount.incrementAndGet();
      }
    }
    // ht.asyncBatch(gets, null, new CustomAsyncBatchCallback(results));

    waitUntilEqual(successCount, errorCount, batchSize, 60 * 1000);
    assertTrue(future.isDone());
    LOG.debug("Succeed get operation number: " + successCount.get());
    LOG.debug("Failed get operation number: " + errorCount.get());
    LOG.debug("Results: " + Arrays.asList(results));

    htable.close();
  }

  @Test(timeout = 60000)
  public void testCallBack() throws Exception {
    final byte[] table = Bytes.toBytes(name.getMethodName());
    final byte[] qualifier = Bytes.toBytes("q");
    HTable htable =
        TEST_UTIL.createTable(table, COLUMN_FAMILY,
          new byte[][] { Bytes.toBytes("thread2"), Bytes.toBytes("thread4") });
    int threadsNum = 5;
    final int operationPerThread = 100;
    int expectedOperationCount = threadsNum * operationPerThread;
    final AtomicInteger successCount = new AtomicInteger(0);
    final AtomicInteger errorCount = new AtomicInteger(0);
    Thread[] opThreads = new Thread[threadsNum];

    // A example of customized call back for put
    final class CustomAsyncMutateCallback<MutateResponse> extends AsyncMutateCallback {
      public CustomAsyncMutateCallback(byte[] row) {
        super(row);
      }

      @Override
      public void processResult(Result result) {
        LOG.debug("Receive one mutate result: " + result);
        if (result.isEmpty()) {
          int opCnt = successCount.incrementAndGet();
          LOG.debug("Current finished mutate op count: " + opCnt);
        } else {
          LOG.warn("Got non-empty result.");
        }
      }

      @Override
      public void processError(Throwable exception) {
        LOG.debug("Receive one mutate failure", exception);
        int opCnt = errorCount.incrementAndGet();
        LOG.debug("Current failed mutate op count: " + opCnt);
      }

      @Override
      public String toString() {
        return "CustomAsyncMutateCallback_" + row;
      }
    }

    // Do put in multiple threads
    for (int i = 0; i < threadsNum; i++) {
      final String threadPrefix = "thread" + i + "-";
      opThreads[i] = new AsyncOperationThread(table, threadPrefix, operationPerThread) {

        @Override
        public void doOperation(AsyncableHTableInterface ht, byte[] row) {
          Put put = new Put(row);
          put.addColumn(COLUMN_FAMILY, qualifier, row);
          try {
            ht.asyncPut(put, new CustomAsyncMutateCallback<Message>(row));
          } catch (IOException e) {
            errorCount.incrementAndGet();
            throw new RuntimeException(e);
          }
          // reserved sync way of put
          // try {
          // ht.put(put);
          // successCount.incrementAndGet();
          // } catch (IOException e) {
          // errorCount.incrementAndGet();
          // throw new RuntimeException(e);
          // }
        }
      };
      opThreads[i].start();
    }
    for (Thread opThread : opThreads) {
      opThread.join();
    }
    waitUntilEqual(successCount, errorCount, expectedOperationCount, 60 * 1000);
    LOG.debug("Succeed put operation number: " + successCount.get());
    LOG.debug("Failed put operation number: " + errorCount.get());
    assertEquals(successCount.get(), TEST_UTIL.countRows(htable));

    // Reset counters
    final AtomicInteger emptyResultCount = new AtomicInteger(0);
    final AtomicInteger normalResultCount = new AtomicInteger(0);
    int expectedEmptyResultCnt = errorCount.get();
    int expectedNormalResultCnt = successCount.get();
    successCount.set(0);
    errorCount.set(0);

    // An example of customized call back for get
    final class CustomAsyncGetCallback<GetResponse> extends AsyncGetCallback {
      public CustomAsyncGetCallback(byte[] row) {
        super(row);
      }

      @Override
      public void processResult(Result result) {
        LOG.debug("Receive one get result: " + result);
        if (result.isEmpty()) {
          int emptyCnt = emptyResultCount.incrementAndGet();
          LOG.warn("Got empty result, empty result count: " + emptyCnt);
        } else {
          int normalCnt = normalResultCount.incrementAndGet();
          LOG.warn("Got normal result, normal result count: " + normalCnt);
        }
        int opCnt = successCount.incrementAndGet();
        LOG.debug("Current finished get op count: " + opCnt);
      }

      @Override
      public void processError(Throwable exception) {
        LOG.debug("Receive one get failure", exception);
        int opCnt = errorCount.incrementAndGet();
        LOG.debug("Current failed get op count: " + opCnt);
      }

      @Override
      public String toString() {
        return "CustomAsyncGetCallback_" + row;
      }
    }

    // Do get in multiple threads
    for (int i = 0; i < threadsNum; i++) {
      final String threadPrefix = "thread" + i + "-";
      opThreads[i] = new AsyncOperationThread(table, threadPrefix, operationPerThread) {

        @Override
        public void doOperation(AsyncableHTableInterface ht, byte[] row) {
          Get get = new Get(row);
          CustomAsyncGetCallback<Message> getCallback = new CustomAsyncGetCallback<Message>(row);
          try {
            ((AsyncableHTableInterface) ht).asyncGet(get, getCallback);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };
      opThreads[i].start();
    }
    for (Thread opThread : opThreads) {
      opThread.join();
    }

    waitUntilEqual(successCount, errorCount, expectedOperationCount, 60 * 1000);
    if (errorCount.get() == 0) {
      assertEquals("Expected " + expectedEmptyResultCnt + " empty results but actually "
              + emptyResultCount.get(), expectedEmptyResultCnt, emptyResultCount.get());
      assertEquals("Expected " + normalResultCount + " normal results but actually "
              + normalResultCount.get(), expectedNormalResultCnt, normalResultCount.get());
    }
    LOG.debug("Succeed get operation number: " + successCount.get());
    LOG.debug("Failed get operation number: " + errorCount.get());

    // reset counters
    successCount.set(0);
    errorCount.set(0);

    // get row counter before delete
    int numOfRows = TEST_UTIL.countRows(htable);

    // do delete in multiple threads
    for (int i = 0; i < threadsNum; i++) {
      final String threadPrefix = "thread" + i + "-";
      opThreads[i] = new AsyncOperationThread(table, threadPrefix, operationPerThread) {

        @Override
        public void doOperation(AsyncableHTableInterface ht, byte[] row) {
          Delete delete = new Delete(row);
          try {
            ht.asyncDelete(delete, new CustomAsyncMutateCallback<Message>(row));
          } catch (IOException e) {
            errorCount.incrementAndGet();
            throw new RuntimeException(e);
          }
        }
      };
      opThreads[i].start();
    }
    for (Thread opThread : opThreads) {
      opThread.join();
    }
    waitUntilEqual(successCount, errorCount, expectedOperationCount, 60 * 1000);
    LOG.debug("Succeed delete operation number: " + successCount.get());
    LOG.debug("Failed delete operation number: " + errorCount.get());
    int numOfRowsAfterDelete = TEST_UTIL.countRows(htable);
    LOG.debug("Rows before delete: " + numOfRows + ", rows after: " + numOfRowsAfterDelete);
    assertTrue(numOfRows > numOfRowsAfterDelete);

    htable.close();
  }

  @Test(timeout = 60000)
  public void testListenableFuture() throws Exception {
    final byte[] table = Bytes.toBytes(name.getMethodName());
    HTable htable = TEST_UTIL.createTable(table, COLUMN_FAMILY,
            new byte[][]{Bytes.toBytes("thread2"), Bytes.toBytes("thread4")});
    final AtomicInteger successCount = new AtomicInteger(0);
    final AtomicInteger errorCount = new AtomicInteger(0);

    class CustomListener implements Listener<Result> {

      String name;
      final AtomicInteger successCount = new AtomicInteger(0);
      final AtomicInteger errorCount = new AtomicInteger(0);

      public CustomListener(String name) {
        this.name = name;
      }

      @Override
      public void onComplete(Result val) {
        successCount.incrementAndGet();
      }

      @Override
      public void onError(Throwable e) {
        errorCount.incrementAndGet();
      }

      public void reset() {
        successCount.set(0);
        errorCount.set(0);
      }
    }

    //List<Listener<Result>> listeners = new ArrayList<CustomListener>();
    final List<CustomListener> listeners = new ArrayList<CustomListener>();
    int listenerNum = 10;
    for (int num = 0; num < listenerNum; num++) {
      listeners.add(new CustomListener("listener-" + num));
    }

    class Reset {
      public void counters() {
        successCount.set(0);
        errorCount.set(0);
        for (CustomListener listener : listeners) {
          listener.reset();
        }
      }
    }

    // Do put in multi threads
    runFutureThreads(table, ActionType.PUT, threadsNum, operationPerThread, successCount,
            errorCount, listeners);
    int rowNum = TEST_UTIL.countRows(htable);
    for (CustomListener listener : listeners) {
      assertEquals(listener.successCount.get(), rowNum);
      assertEquals(listener.errorCount.get(), threadsNum*operationPerThread - rowNum);
    }

    // Reset counters
    new Reset().counters();

    // Do get in multi threads
    runFutureThreads(table, ActionType.GET, threadsNum, operationPerThread, successCount,
            errorCount, listeners);
    for (CustomListener listener : listeners) {
      assertEquals(listener.successCount.get(), TEST_UTIL.countRows(htable));
      assertEquals(listener.errorCount.get(), 0);
    }

    // Reset counters
    new Reset().counters();

    // Do delete in multi threads
    runFutureThreads(table, ActionType.DELETE, threadsNum, operationPerThread, successCount,
            errorCount, listeners);
    for (CustomListener listener : listeners) {
      assertEquals(listener.successCount.get(), threadsNum*operationPerThread);
      assertEquals(listener.errorCount.get(), TEST_UTIL.countRows(htable));
    }

    htable.close();
  }

  @Test(timeout = 60000)
  public void testBatchListenableFuture() throws Exception {
    final byte[] table = Bytes.toBytes(name.getMethodName());
    HTable htable = TEST_UTIL.createTable(table, COLUMN_FAMILY,
            new byte[][]{Bytes.toBytes("thread2"), Bytes.toBytes("thread4")});
    int batchSize = 10;
    final ExecutorService listenerExecutorService = Executors.newCachedThreadPool();

    class CustomListener implements Listener<Object[]> {

      String name;
      final AtomicInteger successCount = new AtomicInteger(0);
      final AtomicInteger errorCount = new AtomicInteger(0);
      Boolean batchFailed = false;

      public CustomListener(String name) {
        this.name = name;
      }

      @Override
      public void onComplete(Object[] vals) {
        for (Object val : vals) {
          if (val == null || val instanceof Throwable) {
            errorCount.incrementAndGet();
            LOG.debug(name + " error count:" + errorCount.get());
          } else {
            successCount.incrementAndGet();
            LOG.debug(name + " success count:" + successCount.get());
          }
        }
      }

      @Override
      public void onError(Throwable e) {
        batchFailed = true;
        LOG.debug(name + " batch opt failed");
      }

      public void reset() {
        successCount.set(0);
        errorCount.set(0);
      }
    }

    final List<CustomListener> listeners = new ArrayList<CustomListener>();
    int listenerNum = 10;
    for (int num = 0; num < listenerNum; num++) {
      listeners.add(new CustomListener("listener-" + num));
    }

    class Reset {
      public void counters() {
        for (CustomListener listener : listeners) {
          listener.reset();
        }
      }
    }

    List<Row> puts = new ArrayList<Row>();
    for (int i = 0; i < batchSize; i++) {
      byte[] row = Bytes.toBytes("thread" + i);
      Put put = new Put(row);
      put.addColumn(COLUMN_FAMILY, qualifier, row);
      puts.add(put);
    }
    Object[] results = new Object[puts.size()];
    AsyncFuture future = htable.asyncBatch(puts, results);
    for (CustomListener listener : listeners) {
      Futures.addListener(future, listener, listenerExecutorService);
    }

    future.get();
    assertTrue(future.isDone());
    for (CustomListener listener : listeners) {
      waitUntilEqual(listener.successCount, listener.errorCount, batchSize, 60 * 1000);
      assertFalse(listener.batchFailed);
      assertEquals(listener.errorCount.get(), 0);
      assertEquals(listener.successCount.get(), TEST_UTIL.countRows(htable));
    }

    // Reset counters
    new Reset().counters();

    List<Row> gets = new ArrayList<Row>();
    for (int i = 0; i < batchSize; i++) {
      byte[] row = Bytes.toBytes("thread" + i);
      Get get = new Get(row);
      gets.add(get);
    }
    results = new Object[gets.size()];
    future = htable.asyncBatch(gets, results);
    for (CustomListener listener : listeners) {
      Futures.addListener(future, listener, listenerExecutorService);
    }

    future.get();
    assertTrue(future.isDone());
    for (CustomListener listener : listeners) {
      waitUntilEqual(listener.successCount, listener.errorCount, batchSize, 60 * 1000);
      assertFalse(listener.batchFailed);
      assertEquals(listener.errorCount.get(), 0);
      assertEquals(listener.successCount.get(), TEST_UTIL.countRows(htable));
    }

    htable.close();
  }

  @Test(timeout = 60000)
  public void testAsyncFuture() throws Exception {
    final byte[] table = Bytes.toBytes(name.getMethodName());
    HTable htable = TEST_UTIL.createTable(table, COLUMN_FAMILY,
      new byte[][] { Bytes.toBytes("thread2"), Bytes.toBytes("thread4") });
    final AtomicInteger successCount = new AtomicInteger(0);
    final AtomicInteger errorCount = new AtomicInteger(0);

    // Do put in multi threads
    runFutureThreads(table, ActionType.PUT, threadsNum, operationPerThread, successCount,
            errorCount, null);
    assertEquals(successCount.get(), TEST_UTIL.countRows(htable));

    // Reset counters
    successCount.set(0);
    errorCount.set(0);

    // Do get in multi threads
    runFutureThreads(table, ActionType.GET, threadsNum, operationPerThread, successCount,
      errorCount, null);

    // Reset counters
    successCount.set(0);
    errorCount.set(0);

    // Do delete in multi threads
    runFutureThreads(table, ActionType.DELETE, threadsNum, operationPerThread, successCount,
            errorCount, null);
    assertEquals(0, TEST_UTIL.countRows(htable));

    htable.close();
  }

  @Test(timeout = 60000)
  public void testFutureGetAfterCancel() throws Exception {
    final byte[] table = Bytes.toBytes(name.getMethodName());
    HTable htable = null;
    try {
      htable = TEST_UTIL.createTable(table, COLUMN_FAMILY,
        new byte[][] { Bytes.toBytes("thread2"), Bytes.toBytes("thread4") });
      byte[] fakeRow = Bytes.toBytes("fakerow");
      // test async get
      Get get = new Get(fakeRow);
      Future<?> future = htable.asyncGet(get);
      getAfterCancel(future);
      // test async put
      Put put = new Put(fakeRow);
      future = htable.asyncPut(put);
      getAfterCancel(future);
      // test async delete
      Delete del = new Delete(fakeRow);
      future = htable.asyncDelete(del);
      getAfterCancel(future);
      // test async batch
      List<Row> rows = new ArrayList<Row>();
      rows.add(get);
      future = htable.asyncBatch(rows);
      getAfterCancel(future);
    } finally {
      if (htable != null) htable.close();
    }
  }

  private void getAfterCancel(Future<?> future)
      throws InterruptedException, ExecutionException, TimeoutException {
    future.cancel(true);
    try {
      future.get(100, TimeUnit.MILLISECONDS);
      fail("Should have thrown DoNotRetryIOException"
          + " when invoking get against already canceled future");
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof DoNotRetryIOException)) {
        fail("Should have thrown DoNotRetryIOException but actually " + e.getCause());
      }
      // test pass
      LOG.debug("Caught expected exception", e);
    }
  }

  @Test(timeout = 60000)
  public void testFutureGetAfterDone() throws Exception {
    final byte[] table = Bytes.toBytes(name.getMethodName());
    HTable htable = null;
    try {
      htable = TEST_UTIL.createTable(table, COLUMN_FAMILY,
        new byte[][] { Bytes.toBytes("thread2"), Bytes.toBytes("thread4") });
      byte[] fakeRow = Bytes.toBytes("fakerow");
      // test async get
      Get get = new Get(fakeRow);
      Future<?> future = htable.asyncGet(get);
      getAfterDone(future);
      // test async put
      Put put = new Put(fakeRow);
      future = htable.asyncPut(put);
      getAfterDone(future);
      // test async delete
      Delete del = new Delete(fakeRow);
      future = htable.asyncDelete(del);
      getAfterDone(future);
      // test async batch
      List<Row> rows = new ArrayList<Row>();
      rows.add(get);
      future = htable.asyncBatch(rows);
      getAfterDone(future);
    } finally {
      if (htable != null) htable.close();
    }
  }

  private void getAfterDone(Future<?> future) {
    Object result = null;
    Exception exception = null;
    try {
      result = future.get();
    } catch (Exception e) {
      exception = e;
    }
    Object secondGetResult = null;
    Exception secondGetException = null;
    try {
      secondGetResult = future.get();
    } catch (Exception e) {
      secondGetException = e;
    }
    assertEquals(result, secondGetResult);
    assertEquals(exception, secondGetException);
    // test pass
    LOG.debug("Result: " + result);
    LOG.debug("Exception", exception);
  }

  @Test(timeout = 60000)
  public void testWithBlockingClient() throws Exception {
    final byte[] table = Bytes.toBytes(name.getMethodName());
    TEST_UTIL.getConfiguration().set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
      RpcClientImpl.class.getName());
    HTable htable = null;
    try {
      htable = TEST_UTIL.createTable(table, COLUMN_FAMILY,
        new byte[][] { Bytes.toBytes("thread2"), Bytes.toBytes("thread4") });
      byte[] fakeRow = Bytes.toBytes("fakerow");
      // test async get
      Get get = new Get(fakeRow);
      Future<?> future = htable.asyncGet(get);
      asyncCallWithBlockingClient(future);
      // test async put
      Put put = new Put(fakeRow);
      future = htable.asyncPut(put);
      asyncCallWithBlockingClient(future);
      // test async delete
      Delete del = new Delete(fakeRow);
      future = htable.asyncDelete(del);
      asyncCallWithBlockingClient(future);
      // test async batch
      List<Row> rows = new ArrayList<Row>();
      rows.add(get);
      future = htable.asyncBatch(rows);
      asyncCallWithBlockingClient(future);
      htable.close();
    } finally {
      if (htable != null) {
        htable.close();
      }
    }
  }

  private void asyncCallWithBlockingClient(Future<?> future)
      throws InterruptedException, ExecutionException, TimeoutException {
    try {
      future.get(100, TimeUnit.MILLISECONDS);
      fail("Should have thrown DoNotRetryIOException"
          + " when sending async call through blocking client");
    } catch (ExecutionException e) {
      if (!(e.getCause() instanceof DoNotRetryIOException)) {
        LOG.debug("Actual exception", e);
        fail("Should have thrown DoNotRetryIOException but actually " + e.getCause());
      }
      // test pass
      LOG.debug("Caught expected exception", e);
    }
  }

  /**
   * Common method to launch future threads and get results asynchronously
   * @param table the table name
   * @param actionType action type to test
   * @param threadsNum how many threads to launch
   * @param operationPerThread how many operations per thread
   * @param successCount counter for success operation
   * @param errorCount counter for failed operation
   * @throws InterruptedException if any thread interrupted
   * @throws IOException if any error occurs
   */
  private <S extends Listener> void runFutureThreads(byte[] table, final ActionType actionType, int threadsNum,
      int operationPerThread, final AtomicInteger successCount,
      final AtomicInteger errorCount, final List<S> listeners) throws InterruptedException, IOException {
    final Collection<AsyncFuture<Result>> futures =
        Collections.synchronizedCollection(new ArrayList<AsyncFuture<Result>>());
    Thread[] opThreads = new Thread[threadsNum];

    final ExecutorService listenerExecutorService = Executors.newCachedThreadPool();
    // launch threads
    for (int i = 0; i < threadsNum; i++) {
      final String threadPrefix = "thread" + i + "-";
      opThreads[i] = new AsyncOperationThread(table, threadPrefix, operationPerThread) {

        @Override
        public void doOperation(AsyncableHTableInterface ht, byte[] row) {
          try {
            AsyncFuture<Result> future = null;
            switch (actionType) {
            case GET:
              Get get = new Get(row);
              future = ht.asyncGet(get);
              break;
            case PUT:
              Put put = new Put(row);
              put.addColumn(COLUMN_FAMILY, qualifier, row);
              future = ht.asyncPut(put);
              break;
            case DELETE:
              Delete delete = new Delete(row);
              future = ht.asyncDelete(delete);
              break;
            }

            if (future != null && listeners != null) {
              for (Listener listener : listeners) {
                Futures.addListener(future, listener, listenerExecutorService);
              }
            }
            futures.add(future);
          } catch (Exception e) {
            LOG.debug("Receive one " + actionType.name() + " failure", e);
            int opCnt = errorCount.incrementAndGet();
            LOG.debug("Current failed " + actionType.name() + " op count: " + opCnt);
          }
        }
      };
      opThreads[i].start();
    }
    for (Thread opThread : opThreads) {
      opThread.join();
    }
    // Get result
    LOG.debug("Start to get " + actionType.name() + " results: " + futures.size());
    for (AsyncFuture<Result> future : futures) {
      try {
        Result result = future.get();
        LOG.debug("Received result: " + result + " for " + actionType.name() + ": " + future);
        int opCnt = successCount.incrementAndGet();
        LOG.debug("Current finished " + actionType.name() + " op count: " + opCnt);
      } catch (Exception e) {
        LOG.debug(actionType.name() + " failed", e);
      }
    }
    // wait until done and check
    waitUntilEqual(successCount, expectedOperationCount, 60 * 1000);
    LOG.debug("Succeed " + actionType.name() + " operation number: " + successCount.get());
  }

  private void waitUntilEqual(AtomicInteger successCount, int expectedCount, int timeout)
      throws IOException {
    long start = System.currentTimeMillis();
    while (successCount.get() != expectedCount) {
      Threads.sleep(10);
      if (System.currentTimeMillis() - start > timeout) {
        throw new IOException("Timeout waiting to " + expectedCount + ",now successCnt:"
            + successCount.get());
      }
    }
  }

  private void waitUntilEqual(AtomicInteger successCount, AtomicInteger failureCount,
      int expectedCount, int timeout) throws IOException {
    long start = System.currentTimeMillis();
    while (successCount.get() + failureCount.get() != expectedCount) {
      Threads.sleep(10);
      if (System.currentTimeMillis() - start > timeout) {
        throw new IOException("Timeout waiting to " + expectedCount + ",now successCnt:"
            + successCount.get() + "; failureCnt: " + failureCount);
      }
    }
  }

  static abstract class AsyncOperationThread extends Thread {
    private static final Log LOG = LogFactory.getLog(AsyncOperationThread.class);

    private final byte[] table;
    private final String threadPrefix;
    private final int opPerThread;

    public AsyncOperationThread(byte[] table, String threadPrefix, int opPerThread) {
      this.table = table;
      this.threadPrefix = threadPrefix;
      this.opPerThread = opPerThread;
    }

    public abstract void doOperation(AsyncableHTableInterface ht, byte[] row);

    public void run() {
      Table ht = null;
      try {
        ht = connection.getTable(TableName.valueOf(table));
        if (ht instanceof AsyncableHTableInterface) {
          for (int i = 0; i < opPerThread; i++) {
            doOperation((AsyncableHTableInterface) ht, Bytes.toBytes(threadPrefix + i));
          }
        }
      } catch (IllegalArgumentException e) {
        LOG.warn("Failed to get TableName instance for table " + Bytes.toString(table), e);
      } catch (IOException e) {
        LOG.warn("Failed to get Table instance for table " + Bytes.toString(table), e);
      } finally {
        try {
          if (ht != null) ht.close();
        } catch (IOException e) {
        }
      }
    }
  }

  enum ActionType {
    GET, PUT, DELETE
  }
}
