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
package org.apache.hadoop.hbase.embedded;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.snapshot.SnapshotDoesNotExistException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestEmbeddedHBase {

  @Rule
  public TestName name = new TestName();

  @Parameterized.Parameter
  public String dbOperatorClass;

  private EmbeddedHBase hbase = null;
  private final Log LOG = LogFactory.getLog(TestEmbeddedHBase.class);

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
            { HTableDBOperator.class.getName() },
            { DirectDBOperator.class.getName() }
    });
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  @Before
  public void setUpBeforeTest() throws Exception {
    Options opts = new Options();
    String testName = name.getMethodName().replaceAll("\\[", "-").replaceAll("\\]", "");
    String path = "/tmp/" + testName;
    opts.setTableName(TableName.valueOf(testName));
    opts.setSplitKeys(EmbeddedHBase.getSplitKeys(10));
    opts.setDurability("async_wal");
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.EMBEDDED_DB_OPERATOR_CLASS, dbOperatorClass);
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024);
    LOG.debug("DBOpeator class: " + dbOperatorClass);
    hbase = EmbeddedHBase.open(opts, path, conf);
  }

  @After
  public void tearDownAfterTest() throws Exception {
    if (hbase != null) {
      hbase.close();
    }
  }

  @Test
  public void testPutAndGet() throws Exception {
    final int THREAD_NUM = 100;
    final byte[] family = Bytes.toBytes("cf");
    final byte[] qualifier = Bytes.toBytes("c1");
    final AtomicInteger successCnt = new AtomicInteger(0);
    ArrayList<Thread> threads = new ArrayList<>(THREAD_NUM);
    for (int i = 0; i < THREAD_NUM; i++) {
      final int index = i;
      Thread t = new Thread(new Runnable() {

        @Override
        public void run() {
          final byte[] row = Bytes.toBytes("row-" + index);
          final byte[] value = Bytes.toBytes("v" + index);
          try {
            Put put = new Put(row);
            put.addColumn(family, qualifier, value);
            hbase.put(put);
            Get get = new Get(row);
            Result result = hbase.get(get);
            byte[] returnedValue = result.getValue(family, qualifier);
            if (Bytes.equals(value, returnedValue)) {
              successCnt.getAndIncrement();
            } else {
              LOG.error("Should be equal but not, original value: " + Bytes.toString(value)
                  + ", returned value: "
                  + (returnedValue == null ? "null" : Bytes.toString(returnedValue)));
            }
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        }
      });
      threads.add(t);
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    Assert.assertEquals(THREAD_NUM, successCnt.get());
  }

  @Test
  public void testAppendAndDelete() throws Exception {
    // FIXME the test fails with more threads, we need to locate the root cause and fix it
    final int THREAD_NUM = 2;
    final byte[] family = Bytes.toBytes("cf");
    final byte[] qualifier = Bytes.toBytes("c1");
    final byte[] row = Bytes.toBytes("row");
    final byte[] value = Bytes.toBytes("v");
    Get g = new Get(row);
    LOG.debug("Result before delete: " + hbase.get(g));
    // cleanup first to avoid dirty state caused by previous failure before append
    Delete delete = new Delete(row);
    hbase.delete(delete);
    LOG.debug("Result before append: " + hbase.get(g));
    // test append
    ArrayList<Thread> threads = new ArrayList<>(THREAD_NUM);
    for (int i = 0; i < THREAD_NUM; i++) {
      Thread t = new Thread(new Runnable() {

        @Override
        public void run() {
          try {
            Append append = new Append(row);
            append.add(family, qualifier, value);
            hbase.append(append);
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        }
      });
      threads.add(t);
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    Get get = new Get(row);
    Result result = hbase.get(get);
    byte[] actualResult = result.getValue(family, qualifier);
    LOG.debug("Result: " + Bytes.toString(actualResult));
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < THREAD_NUM; i++) {
      builder.append("v");
    }
    Assert.assertEquals(builder.toString(), Bytes.toString(actualResult));
    // test delete again
    delete = new Delete(row);
    hbase.delete(delete);
    Threads.sleep(100);
    get = new Get(row);
    Result checkDelete = hbase.get(get);
    LOG.debug("Result after delete: " + Bytes.toString(checkDelete.getValue(family, qualifier)));
    Assert.assertTrue("Should be empty after delete but actually not", checkDelete.isEmpty());
  }

  @Test
  public void testIncrement() throws Exception {
    // FIXME the test fails with more threads, we need to locate the root cause and fix it
    final int THREAD_NUM = 2;
    final byte[] family = Bytes.toBytes("cf");
    final byte[] qualifier = Bytes.toBytes("c1");
    final byte[] row = Bytes.toBytes("row");
    Get g = new Get(row);
    LOG.debug("Result before delete: " + hbase.get(g));
    // cleanup first to avoid dirty state caused by previous failure before append
    Delete delete = new Delete(row);
    hbase.delete(delete);
    LOG.debug("Result before increment: " + hbase.get(g));
    // test append
    ArrayList<Thread> threads = new ArrayList<>(THREAD_NUM);
    for (int i = 0; i < THREAD_NUM; i++) {
      Thread t = new Thread(new Runnable() {

        @Override
        public void run() {
          try {
            Increment incre = new Increment(row);
            incre.addColumn(family, qualifier, 1L);
            hbase.increment(incre);
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        }
      });
      threads.add(t);
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    Get get = new Get(row);
    Result result = hbase.get(get);
    long actualResult = Bytes.toLong(result.getValue(family, qualifier));
    LOG.debug("Result: " + actualResult);
    Assert.assertEquals(THREAD_NUM, actualResult);
    // test delete again
    delete = new Delete(row);
    hbase.delete(delete);
    Threads.sleep(100);
    get = new Get(row);
    Result checkDelete = hbase.get(get);
    LOG.debug("Result after delete: " + Bytes.toString(checkDelete.getValue(family, qualifier)));
    Assert.assertTrue("Should be empty after delete but actually not", checkDelete.isEmpty());
  }

  @Test
  public void testScan() throws Exception {
    final int THREAD_NUM = 10;
    final int DATA_NUM = 100;
    final int INTER = (int) Math.ceil(1.0 * DATA_NUM/THREAD_NUM);
    final byte[] family = Bytes.toBytes("cf");
    final byte[] qualifier = Bytes.toBytes("c1");

    final AtomicInteger successCnt = new AtomicInteger(0);
    ArrayList<Thread> threads = new ArrayList<>(THREAD_NUM);
    for (int i = 0; i < THREAD_NUM; i++) {
      final int threadId = i;
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          int start = INTER * threadId;
          int end = Math.min(INTER * (threadId + 1), DATA_NUM);

          for (int j = start; j < end; j++) {
            final byte[] row = Bytes.toBytes(String.format("user%04d", j));
            final byte[] value = Bytes.toBytes("v" + j);
            try {
              Put put = new Put(row);
              put.addColumn(family, qualifier, value);
              hbase.put(put);

              Scan scan = new Scan(row);
              scan.setFilter(new SingleColumnValueFilter("cf".getBytes(), "c1".getBytes(), CompareFilter.CompareOp.EQUAL, value));
              ResultScanner scanner = hbase.getResultScanner(scan);
              Result result = scanner.next();
              if (result == null) {
                LOG.error("Should be equal but not, row: " + Bytes.toString(row)
                    + "original value: " + Bytes.toString(value)
                    + ", returned value: null");
                continue;
              }
              byte[] returnedValue = result.getValue(family, qualifier);
              if (Bytes.equals(value, returnedValue)) {
                successCnt.getAndIncrement();
              } else {
                LOG.error("Should be equal but not, row: " + Bytes.toString(row)
                        + "original value: " + Bytes.toString(value)
                        + ", returned value: "
                        + (returnedValue == null ? "null" : Bytes.toString(returnedValue)));
              }

              Thread.sleep(100); //for low pressure in put
            } catch (Throwable e) {
              throw new RuntimeException(e);
            }
          }
        }
      });
      threads.add(t);
    }

    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    Assert.assertEquals(DATA_NUM, successCnt.get());

    ResultScanner scanner = null;


    try {
      for (int i = 0; i <= DATA_NUM; i++) {
        Scan scan = new Scan(Bytes.toBytes(String.format("user%04d", i)));
        scanner = hbase.getResultScanner(scan);

        Result result = null;
        int count = 0;
        while ((result = scanner.next()) != null) {
          count++;
        }

        Assert.assertEquals(DATA_NUM - i, count);
      }
    } finally {
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
    }

    // empty start row for reverse scan
    try {
      Scan emptyRowScan = new Scan(HConstants.EMPTY_START_ROW);
      emptyRowScan.setReversed(true);
      scanner = hbase.getResultScanner(emptyRowScan);

      Result result = null;
      int count = 0;
      while ((result = scanner.next()) != null) {
        count++;
      }

      Assert.assertEquals(DATA_NUM, count);
    } finally {
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
    }

    // Reverse scan test
    try {
      for (int i = 0; i <= DATA_NUM; i++) {
        Scan scan = new Scan(Bytes.toBytes(String.format("user%04d", i)));
        scan.setReversed(true);
        scanner = hbase.getResultScanner(scan);

        Result result = null;
        int count = 0;
        while ((result = scanner.next()) != null) {
          count++;
        }

        if (i < DATA_NUM) {
          Assert.assertEquals(i + 1, count);
        } else {
          Assert.assertEquals(DATA_NUM, count);
        }
      }
    } finally {
      if (scanner != null) {
        scanner.close();
        scanner = null;
      }
    }
  }

  @Test(timeout = 60000)
  public void testSnapshot() throws Exception {
    final int ROW_NUM = 100;
    final byte[] family = Bytes.toBytes("cf");
    final byte[] qualifier = Bytes.toBytes("c1");
    final String snapshotName = name.getMethodName().replaceAll("\\[", "-").replaceAll("\\]", "");
    // Put some data
    for (int i = 0; i < ROW_NUM; i++) {
      byte[] row = Bytes.toBytes("row-" + i);
      byte[] value = Bytes.toBytes("v-" + i);
      Put put = new Put(row);
      put.addColumn(family, qualifier, value);
      hbase.put(put);
    }
    // cleanup first to avoid dirty state caused by previous failure before snapshot
    try {
      hbase.deleteSnapshot(snapshotName);
      LOG.debug("Snapshot deleted: " + snapshotName);
    } catch (SnapshotDoesNotExistException e) {
      // OK if snapshot doesn't exist
    }
    // do snapshot
    hbase.snapshot(snapshotName);
    // do some modification
    final byte[] rowAfterSnapshot = Bytes.toBytes("rowAfterSnapshot");
    Put put = new Put(rowAfterSnapshot);
    put.addColumn(family, qualifier, rowAfterSnapshot);
    hbase.put(put);
    Assert.assertArrayEquals(rowAfterSnapshot,
        hbase.get(new Get(rowAfterSnapshot)).getValue(family, qualifier));
    // do restore
    hbase.restoreSnapshot(snapshotName);
    // make sure data restored
    Assert.assertTrue(hbase.get(new Get(rowAfterSnapshot)).isEmpty());
    Assert.assertArrayEquals(Bytes.toBytes("v-1"),
      hbase.get(new Get(Bytes.toBytes("row-1"))).getValue(family, qualifier));
  }
}
