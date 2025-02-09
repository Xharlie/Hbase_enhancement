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

package org.apache.hadoop.hbase.master.procedure;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(LargeTests.class)
public class TestWALProcedureStoreOnHDFS {
  private static final Log LOG = LogFactory.getLog(TestWALProcedureStoreOnHDFS.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private WALProcedureStore store;

  private ProcedureStore.ProcedureStoreListener stopProcedureListener = new ProcedureStore.ProcedureStoreListener() {
    @Override
    public void postSync() {}

    @Override
    public void abortProcess() {
      LOG.fatal("Abort the Procedure Store");
      store.stop(true);
    }
  };

  private static void initConfig(Configuration conf) {
    conf.setInt("dfs.replication", 3);
    conf.setInt("dfs.namenode.replication.min", 3);

    // increase the value for slow test-env
    conf.setInt("hbase.procedure.store.wal.wait.before.roll", 1000);
    conf.setInt("hbase.procedure.store.wal.max.roll.retries", 5);
    conf.setInt("hbase.procedure.store.wal.sync.failure.roll.max", 5);
  }

  public void setup() throws Exception {
    MiniDFSCluster dfs = UTIL.startMiniDFSCluster(3);

    Path logDir = new Path(new Path(dfs.getFileSystem().getUri()), "/test-logs");
    store = ProcedureTestingUtility.createWalStore(UTIL.getConfiguration(), dfs.getFileSystem(), logDir);
    store.registerListener(stopProcedureListener);
    store.start(8);
    store.recoverLease();
  }

  public void tearDown() throws Exception {
    store.stop(false);
    UTIL.getDFSCluster().getFileSystem().delete(store.getLogDir(), true);

    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test(timeout=60000, expected=RuntimeException.class)
  public void testWalAbortOnLowReplication() throws Exception {
    initConfig(UTIL.getConfiguration());
    setup();
    try {
      assertEquals(3, UTIL.getDFSCluster().getDataNodes().size());

      LOG.info("Stop DataNode");
      UTIL.getDFSCluster().stopDataNode(0);
      assertEquals(2, UTIL.getDFSCluster().getDataNodes().size());

      store.insert(new TestProcedure(1, -1), null);
      for (long i = 2; store.isRunning(); ++i) {
        assertEquals(2, UTIL.getDFSCluster().getDataNodes().size());
        store.insert(new TestProcedure(i, -1), null);
        Thread.sleep(100);
      }
      assertFalse(store.isRunning());
      fail("The store.insert() should throw an exeption");
    } finally {
      tearDown();
    }
  }

  @Test(timeout=60000)
  public void testWalAbortOnLowReplicationWithQueuedWriters() throws Exception {
    initConfig(UTIL.getConfiguration());
    setup();
    try {
      assertEquals(3, UTIL.getDFSCluster().getDataNodes().size());
      store.registerListener(new ProcedureStore.ProcedureStoreListener() {
        @Override
        public void postSync() {
          Threads.sleepWithoutInterrupt(2000);
        }

        @Override
        public void abortProcess() {}
      });

      final AtomicInteger reCount = new AtomicInteger(0);
      Thread[] thread = new Thread[store.getNumThreads() * 2 + 1];
      for (int i = 0; i < thread.length; ++i) {
        final long procId = i + 1;
        thread[i] = new Thread() {
          public void run() {
            try {
              LOG.debug("[S] INSERT " + procId);
              store.insert(new TestProcedure(procId, -1), null);
              LOG.debug("[E] INSERT " + procId);
            } catch (RuntimeException e) {
              reCount.incrementAndGet();
              LOG.debug("[F] INSERT " + procId + ": " + e.getMessage());
            }
          }
        };
        thread[i].start();
      }

      Thread.sleep(1000);
      LOG.info("Stop DataNode");
      UTIL.getDFSCluster().stopDataNode(0);
      assertEquals(2, UTIL.getDFSCluster().getDataNodes().size());

      for (int i = 0; i < thread.length; ++i) {
        thread[i].join();
      }

      assertFalse(store.isRunning());
      assertTrue(reCount.toString(), reCount.get() >= store.getNumThreads() &&
                                     reCount.get() < thread.length);
    } finally {
      tearDown();
    }
  }

  @Test(timeout=60000)
  public void testWalRollOnLowReplication() throws Exception {
    initConfig(UTIL.getConfiguration());
    UTIL.getConfiguration().setInt("dfs.namenode.replication.min", 1);
    setup();
    try {
      int dnCount = 0;
      store.insert(new TestProcedure(1, -1), null);
      UTIL.getDFSCluster().restartDataNode(dnCount);
      for (long i = 2; i < 100; ++i) {
        store.insert(new TestProcedure(i, -1), null);
        waitForNumReplicas(3);
        Thread.sleep(100);
        if ((i % 30) == 0) {
          LOG.info("Restart Data Node");
          UTIL.getDFSCluster().restartDataNode(++dnCount % 3);
        }
      }
      assertTrue(store.isRunning());
    } finally {
      tearDown();
    }
  }

  public void waitForNumReplicas(int numReplicas) throws Exception {
    while (UTIL.getDFSCluster().getDataNodes().size() < numReplicas) {
      Thread.sleep(100);
    }

    for (int i = 0; i < numReplicas; ++i) {
      for (DataNode dn: UTIL.getDFSCluster().getDataNodes()) {
        while (!dn.isDatanodeFullyStarted()) {
          Thread.sleep(100);
        }
      }
    }
  }
}
