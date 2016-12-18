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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HealthChecker.HealthCheckerExitStatus;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.tool.HealthCheck;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.util.Shell;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestNodeHealthCheckChore {

  private static final Log LOG = LogFactory.getLog(TestNodeHealthCheckChore.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final int SCRIPT_TIMEOUT = 5000;
  private File healthScriptFile;
  private String eol = System.getProperty("line.separator");

  @After
  public void cleanUp() throws IOException {
    // delete and recreate the test directory, ensuring a clean test dir between tests
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    fs.delete(testDir, true);
    if (!fs.mkdirs(testDir)) throw new IOException("Failed mkdir " + testDir);
  }

  @Test(timeout=60000)
  public void testHealthCheckerSuccess() throws Exception {
    String normalScript = "echo \"I am all fine\"";
    healthCheckerTest(normalScript, HealthCheckerExitStatus.SUCCESS);
  }

  @Test(timeout=60000)
  public void testHealthCheckerFail() throws Exception {
    String errorScript = "echo ERROR" + eol + "echo \"Node not healthy\"";
    healthCheckerTest(errorScript, HealthCheckerExitStatus.FAILED);
  }

  @Test(timeout=60000)
  public void testHealthCheckerTimeout() throws Exception {
    String timeOutScript = "sleep 10" + eol + "echo \"I am fine\"";
    healthCheckerTest(timeOutScript, HealthCheckerExitStatus.TIMED_OUT);
  }

  public void healthCheckerTest(String script, HealthCheckerExitStatus expectedStatus)
      throws Exception {
    Configuration config = getConfForNodeHealthScript();
    config.addResource(healthScriptFile.getName());
    String location = healthScriptFile.getAbsolutePath();
    long timeout = config.getLong(HConstants.HEALTH_SCRIPT_TIMEOUT, SCRIPT_TIMEOUT);

    HealthChecker checker = new HealthChecker();
    checker.init(location, timeout, 0, 0, 0);

    createScript(script, true);
    HealthReport report = checker.checkHealth();
    assertEquals(expectedStatus, report.getStatus());

    LOG.info("Health Status:" + report.getHealthReport());

    this.healthScriptFile.delete();
  }

  @Test(timeout=60000)
  public void testRSHealthChore() throws Exception{
    Stoppable stop = new StoppableImplementation();
    Configuration conf = getConfForNodeHealthScript();
    String errorScript = "echo ERROR" + eol + " echo \"Server not healthy\"";
    createScript(errorScript, true);
    HealthCheckChore rsChore = new HealthCheckChore(100, stop, conf);
    try {
      //Default threshold is three.
      rsChore.chore();
      rsChore.chore();
      assertFalse("Stoppable must not be stopped.", stop.isStopped());
      rsChore.chore();
      assertTrue("Stoppable must have been stopped.", stop.isStopped());
    } finally {
      stop.stop("Finished w/ test");
    }
  }

  private void createScript(String scriptStr, boolean setExecutable)
      throws Exception {
    if (!this.healthScriptFile.exists()) {
      if (!healthScriptFile.createNewFile()) {
        throw new IOException("Failed create of " + this.healthScriptFile);
      }
    }
    PrintWriter pw = new PrintWriter(new FileOutputStream(healthScriptFile));
    try {
      pw.println(scriptStr);
      pw.flush();
    } finally {
      pw.close();
    }
    healthScriptFile.setExecutable(setExecutable);
    LOG.info("Created " + this.healthScriptFile + ", executable=" + setExecutable);
  }

  private Configuration getConfForNodeHealthScript() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    File tempDir = new File(UTIL.getDataTestDir().toString());
    if (!tempDir.exists()) {
      if (!tempDir.mkdirs()) {
        throw new IOException("Failed mkdirs " + tempDir);
      }
    }
    String scriptName = "HealthScript" + UUID.randomUUID().toString()
        + (Shell.WINDOWS ? ".cmd" : ".sh");
    healthScriptFile = new File(tempDir.getAbsolutePath(), scriptName);
    conf.set(HConstants.HEALTH_SCRIPT_LOC, healthScriptFile.getAbsolutePath());
    conf.setLong(HConstants.HEALTH_FAILURE_THRESHOLD, 3);
    conf.setLong(HConstants.HEALTH_SCRIPT_TIMEOUT, SCRIPT_TIMEOUT);
    return conf;
  }

  /**
   * Simple helper class that just keeps track of whether or not its stopped.
   */
  private static class StoppableImplementation implements Stoppable {
    private volatile boolean stop = false;

    @Override
    public void stop(String why) {
      this.stop = true;
    }

    @Override
    public boolean isStopped() {
      return this.stop;
    }

  }

  /**
   * Test Health Check
   */
  @Test(timeout=60000)
  public void testHealthCheckWithNoOnlineRegions() throws Exception {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.assignment.usezk", true);
    try {
      TEST_UTIL.startMiniCluster(1);
      String table = "testHealthCheck";
      MiniHBaseCluster mini = TEST_UTIL.getMiniHBaseCluster();
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 10);
      TEST_UTIL.waitUntilAllRegionsAssigned(TableName.valueOf(table));
      mini.startRegionServer();

      HealthCheck hc = new HealthCheck(conf);

      int i = 0;
      while (i < 2) {
        String serverName = mini.getRegionServer(i++).getServerName().getServerName();
        String[] args = {"3","0.8","3","20000",serverName};
        hc.initialize(args);
        assertTrue(hc.probeLocalHost());
      }
    } catch (Exception e) {
      // TODO: handle exception
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test(timeout=60000)
  public void testHealthCheckSelectRegions() {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.assignment.usezk", true);
    try {
      TEST_UTIL.startMiniCluster(1);
      String table = "testHealthCheck";
      MiniHBaseCluster mini = TEST_UTIL.getMiniHBaseCluster();
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 10);
      TEST_UTIL.waitUntilAllRegionsAssigned(TableName.valueOf(table));
      mini.startRegionServer();
      HealthCheck hc = new HealthCheck(conf);

      int i = 0;
      while (i < 2) {
        String serverName = mini.getRegionServer(i++).getServerName().getServerName();
        String[] args = {"3","0.8","3","20000",serverName};
        hc.initialize(args);
        List<byte[]> list = hc.getOnlineRegions();
        if (list != null && !list.isEmpty()) {
          int actualRegionSize = list.size();
          assertTrue(actualRegionSize >= 10);
          hc.setSampledRegionCount(3);
          assertEquals(hc.selectProbeRegions(list).size(), 3);
          hc.setSampledRegionCount(-1);
          assertEquals(hc.selectProbeRegions(list).size(), 0);
          hc.setSampledRegionCount(2000);
          //should be 0 due to #EHB-416
          assertEquals(0,hc.selectProbeRegions(list).size());
         }
      }
    } catch (Exception e) {
      // TODO: handle exception
    } finally {
      try {
        TEST_UTIL.shutdownMiniCluster();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Test(timeout=60000)
  public void testHealthCheckWithRSKilledBeforeInit() {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.assignment.usezk", true);
    try {
      TEST_UTIL.startMiniCluster(1);
      String table = "testHealthCheck";
      MiniHBaseCluster mini = TEST_UTIL.getMiniHBaseCluster();
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 10);
      TEST_UTIL.waitUntilAllRegionsAssigned(TableName.valueOf(table));
      HealthCheck hc1 = new HealthCheck(conf);

      String serverName = mini.getRegionServer(0).getServerName().getServerName();
      String[] args = {"3","0.8","3","10000",serverName};
      hc1.initialize(args);
      assertTrue(hc1.probeLocalHost());

      //killed all rs and probe it
      mini.abortRegionServer(0);
      mini.waitForRegionServerToStop(mini.getRegionServer(0).getServerName(), 10000);

      HealthCheck hc2 = new HealthCheck(conf);
      hc2.initialize(args);
      hc2.probeLocalHost();

    } catch (NullPointerException | IllegalArgumentException e) {
      assertTrue(true);
    } catch (Exception e) {
      // TODO: handle exception
    }finally {
      try {
        TEST_UTIL.shutdownMiniCluster();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  @Test(timeout=60000)
  public void testHealthCheckWithRSKilledAfterInit() {
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.assignment.usezk", true);
    try {
      TEST_UTIL.startMiniCluster(1);
      String table = "testHealthCheck";
      MiniHBaseCluster mini = TEST_UTIL.getMiniHBaseCluster();
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 10);
      TEST_UTIL.waitUntilAllRegionsAssigned(TableName.valueOf(table));
      HealthCheck hc1 = new HealthCheck(conf);

      String serverName = mini.getRegionServer(0).getServerName().getServerName();
      String[] args = {"3","0.8","3","10000",serverName};
      hc1.initialize(args);
      assertTrue(hc1.probeLocalHost());

      HealthCheck hc2 = new HealthCheck(conf);
      hc2.initialize(args);

      //killed all rs and probe it
      mini.abortRegionServer(0);
      mini.waitForRegionServerToStop(mini.getRegionServer(0).getServerName(), 10000);

      boolean succeed = hc2.probeLocalHost();

      assertEquals(succeed, false);
    } catch (NullPointerException | IllegalArgumentException e) {
      assertTrue(true);
    } catch (Exception e) {
    } finally {
      try {
        TEST_UTIL.shutdownMiniCluster();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
}
