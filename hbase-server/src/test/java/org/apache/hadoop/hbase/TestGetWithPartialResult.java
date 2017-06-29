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
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.AsyncFuture;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.master.HMaster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * These tests are focused on testing how partial results appear to a client. Partial results are
 * {@link Result}s that contain only a portion of a row's complete list of cells. Partial results
 * are formed when the server breaches its maximum result size when trying to service a client's RPC
 * request. It is the responsibility of the scanner on the client side to recognize when partial
 * results have been returned and to take action to form the complete results.
 * <p>
 * Unless the flag {@link Scan#setAllowPartialResults(boolean)} has been set to true, the caller of
 * {@link ResultScanner#next()} should never see partial results.
 */
@Category(MediumTests.class)
public class TestGetWithPartialResult {
  private static final Log LOG = LogFactory.getLog(TestGetWithPartialResult.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HMaster master = null;
  private final static int MINICLUSTER_SIZE = 5;
  private static Table TABLE = null;

  /**
   * Table configuration
   */
  private static TableName TABLE_NAME = TableName.valueOf("testTable");

  private static int NUM_ROWS = 5;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[][] ROWS = HTestConst.makeNAscii(ROW, NUM_ROWS);

  // Should keep this value below 10 to keep generation of expected kv's simple. If above 10 then
  // table/row/cf1/... will be followed by table/row/cf10/... instead of table/row/cf2/... which
  // breaks the simple generation of expected kv's
  private static int NUM_FAMILIES = 10;
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, NUM_FAMILIES);

  private static int NUM_QUALIFIERS = 1000;
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, NUM_QUALIFIERS);

  private static int VALUE_SIZE = 1024;
  private static byte[] VALUE = Bytes.createMaxByteArray(VALUE_SIZE);

  private static int NUM_COLS = NUM_FAMILIES * NUM_QUALIFIERS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(MINICLUSTER_SIZE);
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    master = cluster.getMaster();
    TEST_UTIL.getHBaseAdmin().setBalancerRunning(false, true);
    TABLE = createTestTable(TABLE_NAME, ROWS, FAMILIES, QUALIFIERS, VALUE);
  }

  static Table createTestTable(TableName name, byte[][] rows, byte[][] families,
      byte[][] qualifiers, byte[] cellValue) throws IOException {
    Table ht = TEST_UTIL.createTable(name, families);
    List<Put> puts = createPuts(rows, families, qualifiers, cellValue);
    ht.put(puts);

    return ht;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }


  /**
   * Make puts to put the input value into each combination of row, family, and qualifier
   * @param rows
   * @param families
   * @param qualifiers
   * @param value
   * @return
   * @throws IOException
   */
  static ArrayList<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
      byte[] value) throws IOException {
    Put put;
    ArrayList<Put> puts = new ArrayList<>();

    for (int row = 0; row < rows.length; row++) {
      put = new Put(rows[row]);
      for (int fam = 0; fam < families.length; fam++) {
        for (int qual = 0; qual < qualifiers.length; qual++) {
          KeyValue kv = new KeyValue(rows[row], families[fam], qualifiers[qual], qual, value);
          put.add(kv);
        }
      }
      puts.add(put);
    }

    return puts;
  }

  /**
   * Compares two results and fails the test if the results are different
   * @param r1
   * @param r2
   * @param message
   */
  static void compareResults(Result r1, Result r2, final String message) {
    if (LOG.isInfoEnabled()) {
      if (message != null) LOG.info(message);
      LOG.info("r1: " + r1);
      LOG.info("r2: " + r2);
    }

    final String failureMessage = "Results r1:" + r1 + " \nr2:" + r2 + " are not equivalent";
    if (r1 == null && r2 == null) fail(failureMessage);
    else if (r1 == null || r2 == null) fail(failureMessage);

    try {
      Result.compareResults(r1, r2);
    } catch (Exception e) {
      fail(failureMessage);
    }
  }


  private void moveRegion(Table table, int index) throws IOException{
    List<Pair<HRegionInfo, ServerName>> regions = MetaTableAccessor
        .getTableRegionsAndLocations(master.getZooKeeper(), TEST_UTIL.getConnection(),
            table.getName());
    assertEquals(1, regions.size());
    HRegionInfo regionInfo = regions.get(0).getFirst();
    ServerName name = TEST_UTIL.getHBaseCluster().getRegionServer(index).getServerName();
    TEST_UTIL.getHBaseAdmin().move(regionInfo.getEncodedNameAsBytes(),
        Bytes.toBytes(name.getServerName()));
  }

  @Test
  public void testGetLargeRow() throws IOException, InterruptedException, ExecutionException {

	moveRegion(TABLE,1);
    
    Get get = new Get(ROWS[1]);
	Result rs1 = ((HTable)TABLE).get(get);
	assertEquals(NUM_COLS, rs1.size());
	
	moveRegion(TABLE,2);
	Result rs2 = ((HTable)TABLE).get(get);
	assertEquals(NUM_COLS, rs2.size());
	
	compareResults(rs1, rs2, "the two result should be equal!");
  }
}
