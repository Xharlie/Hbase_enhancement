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
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.tool.DirectHealthCheck;
import org.apache.hadoop.hbase.tool.HealthCheck;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.util.Shell;
import org.junit.*;
import org.junit.experimental.categories.Category;

/**
 * Created by xharlie on 8/16/16.
 */
@Category(SmallTests.class)
public class TestNodeHealthCheckChoreDirect {
  private static final Log LOG = LogFactory.getLog(TestNodeHealthCheckChore.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int HEALTH_DIRECTCHECK_TIMEOUT = 5000;
  private static Configuration conf;

  @After
  public void tearDown() throws Exception {
    // delete and recreate the test directory, ensuring a clean test dir between tests
    TEST_UTIL.shutdownMiniCluster();
    Path testDir = TEST_UTIL.getDataTestDir();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    fs.delete(testDir, true);
    if (!fs.mkdirs(testDir)) throw new IOException("Failed mkdir " + testDir);
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.assignment.usezk", true);
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }
  /**
   * Test Health Check
   */
  @Test(timeout = 60000)
  public void testHealthCheckWithNoOnlineRegions() throws Exception {
    MiniHBaseCluster mini = TEST_UTIL.getMiniHBaseCluster();
    try {
      String table = "testHealthCheck";
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(desc, null);
      TEST_UTIL.waitUntilAllRegionsAssigned(TableName.valueOf(table));
      mini.startRegionServer();
      DirectHealthCheck dhc;
      DirectHealthChecker dhcer = new DirectHealthChecker();
      int i = 0;
      while (i < 2) {
        HRegionServer rs = mini.getRegionServer(i++);
        String serverName = rs.getServerName().getServerName();
        dhcer.init(40000, 3, 0.8f, 3, 20000, serverName, rs, conf);
        assertTrue(dhcer.checkHealthFromOutSide());
      }
    } catch (Exception e) {
      // TODO: handle exception
      int i = 0;
      while (i < 2) {
        mini.abortRegionServer(i);
        mini.waitForRegionServerToStop(mini.getRegionServer(i++).getServerName(), 10000);
      }
    }
  }

  @Test(timeout = 60000)
  public void testHealthCheckSelectRegions() {
    MiniHBaseCluster mini = TEST_UTIL.getMiniHBaseCluster();
    try {
      String table = "testHealthCheck";
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 10);
      TEST_UTIL.waitUntilAllRegionsAssigned(TableName.valueOf(table));
      mini.startRegionServer();
      int i = 0;
      while (i < 2) {
        DirectHealthChecker dhcer = new DirectHealthChecker();
        HRegionServer rs = mini.getRegionServer(i++);
        String serverName = rs.getServerName().getServerName();
        dhcer.init(40000, 3, 0.8f, 3, 20000, serverName, rs, conf);
        DirectHealthCheck dhc = dhcer.getDirectHealthCheck();
        List<Region> list = dhc.getOnlineRegions();
        if (list != null && !list.isEmpty()) {
          int actualRegionSize = list.size();
          assertTrue(actualRegionSize >= 10);
          dhc.setSampledRegionCount(3);
          assertEquals(dhc.selectProbeRegions(list).size(), 3);
          dhc.setSampledRegionCount(-1);
          assertEquals(dhc.selectProbeRegions(list).size(), 0);
          dhc.setSampledRegionCount(2000);
          //should be 0 due to #EHB-416
          assertEquals(0, dhc.selectProbeRegions(list).size());
        }
      }
    } catch (Exception e) {
      // TODO: handle exception
    }
  }

  @Test(timeout = 60000)
  public void testHealthCheckWithRSKilledBeforeInit() {
    MiniHBaseCluster mini = TEST_UTIL.getMiniHBaseCluster();
    try {
      String table = "testHealthCheck";
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 10);
      TEST_UTIL.waitUntilAllRegionsAssigned(TableName.valueOf(table));
      DirectHealthChecker dhcer = new DirectHealthChecker();
      HRegionServer rs = mini.getRegionServer(0);
      String serverName = rs.getServerName().getServerName();
      dhcer.init(60000, 3, 0.8f, 3, 40000, serverName, rs, conf);
      assertTrue(dhcer.checkHealthFromOutSide());

      //killed all rs and probe it
      mini.abortRegionServer(0);
      mini.waitForRegionServerToStop(mini.getRegionServer(0).getServerName(), 10000);

      dhcer.init(40000, 3, 0.8f, 3, 20000, serverName, rs, conf);
      assertTrue(dhcer.checkHealthFromOutSide());

    } catch (NullPointerException | IllegalArgumentException e) {
      assertTrue(true);
    } catch (Exception e) {
      // TODO: handle exception
    }
  }

  @Test(timeout = 160000)
  public void testHealthCheckWithRSKilledAfterInit() {
    MiniHBaseCluster mini = TEST_UTIL.getMiniHBaseCluster();
    try {
      String table = "testHealthCheck";
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table));
      desc.addFamily(new HColumnDescriptor("cf"));
      admin.createTable(desc, Bytes.toBytes("A"), Bytes.toBytes("Z"), 10);
      TEST_UTIL.waitUntilAllRegionsAssigned(TableName.valueOf(table));
      DirectHealthChecker dhcer = new DirectHealthChecker();
      HRegionServer rs = mini.getRegionServer(0);
      String serverName = rs.getServerName().getServerName();
      dhcer.init(40000, 3, 0.8f, 3, 20000, serverName, rs, conf);
      assertTrue(dhcer.checkHealthFromOutSide());
      LOG.info("pass health check");
      List<Region> regions = rs.getOnlineRegions();
      //killed all rs and probe it
      mini.abortRegionServer(0);
      mini.waitForRegionServerToStop(mini.getRegionServer(0).getServerName(), 10000);
      assertFalse(dhcer.checkHealthFromOutSide(regions));
      LOG.info("failed health check");
    } catch (NullPointerException | IllegalArgumentException e) {
      LOG.info("Health check get exception", e);
      assertTrue(false);
    } catch (Exception e) {
    }
  }
}
