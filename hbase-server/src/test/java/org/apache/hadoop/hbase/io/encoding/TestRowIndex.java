/*
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
package org.apache.hadoop.hbase.io.encoding;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * A client-side test, mostly testing scanners with various parameters.
 */
@Category(MediumTests.class)
@RunWith(Parameterized.class)
public class TestRowIndex {
  private static final Log LOG = LogFactory.getLog(TestRowIndex.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String ROW = "row";
  private static byte[] FAMILY = Bytes.toBytes("cf");
  private static byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static String VALUE = "value";

  private final DataBlockEncoding encoding;

  @Parameters
  public static Collection<Object[]> parameters() {
    List<Object[]> paramList = new ArrayList<Object[]>();
    paramList.add(new Object[] { DataBlockEncoding.ROW_INDEX_V1});
    paramList.add(new Object[] { DataBlockEncoding.ROW_INDEX_V2});
    return paramList;
  }

  public TestRowIndex(DataBlockEncoding encoding) {
    this.encoding = encoding;
  }

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, 10 * 1024 * 1024);
    TEST_UTIL.startMiniCluster(3);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  @Test
  public void testRowIndex() throws Exception {
    TableName TABLE = TableName.valueOf("testRowIndex");
    HTableDescriptor desc = new HTableDescriptor(TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    hcd.setMaxVersions(20);
    hcd.setDataBlockEncoding(encoding);
    desc.addFamily(hcd);
    TEST_UTIL.getHBaseAdmin().createTable(desc);

    Table ht = TEST_UTIL.getConnection().getTable(TABLE);
    Put put;
    int numRows = 10;
    
    for (int row = 0; row < numRows; ++row) {
      byte[] rowByte = Bytes.toBytes(ROW + "" + row);
      for (int version = 0; version < row + 1; ++version) {
        put = new Put(rowByte);
        KeyValue kv = new KeyValue(rowByte, FAMILY, QUALIFIER, Bytes.toBytes(VALUE + "" + version));
        put.add(kv);
        System.out.println(kv.toString() + ", value: " + Bytes.toString(kv.getValue()));
        ht.put(put);
      }
    }

    TEST_UTIL.flush(TABLE);
    Thread.sleep(10000);
    
    for (int row = 0; row < numRows; row++) {
      byte[] rowByte = Bytes.toBytes(ROW + "" + row);
      Get get = new Get(rowByte);
      Result result = ht.get(get);
      for (Cell cell : result.rawCells()) {
        System.out.println(cell.toString() + ", value: "+ Bytes.toString(cell.getValue()));
      }
      assertEquals(VALUE + "" + row, Bytes.toString(result.getValue(FAMILY, QUALIFIER)));
    }
    
    Scan scan = new Scan();
    ResultScanner scanner = ht.getScanner(scan);
    for (int row = 0; row < numRows; row++) {
      Result result = scanner.next();
      for (Cell cell : result.rawCells()) {
        System.out.println(cell.toString() + ", value: "+ Bytes.toString(cell.getValue()));
      }
      assertEquals(VALUE + "" + row, Bytes.toString(result.getValue(FAMILY, QUALIFIER)));
    }
    scanner.close();
    TEST_UTIL.getHBaseAdmin().disableTable(TABLE);
    TEST_UTIL.getHBaseAdmin().deleteTable(TABLE);
  }

}
