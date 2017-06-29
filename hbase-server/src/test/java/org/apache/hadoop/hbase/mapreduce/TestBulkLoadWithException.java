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
package org.apache.hadoop.hbase.mapreduce;

import java.util.Random;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.FileNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.hbase.regionserver.controller.FlushThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.controller.PressureAwareFlushThroughputController;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test cases for the bulkload when flush timeout
 */
@Category(LargeTests.class)
public class TestBulkLoadWithException {
  private static final byte[] QUALIFIER = Bytes.toBytes("myqual");
  private static final byte[] FAMILY = Bytes.toBytes("myfam");
  private static final String NAMESPACE = "bulkNS";

  static final int MAX_FILES_PER_REGION_PER_FAMILY = 4;

  static HBaseTestingUtility util = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,"");
    util.getConfiguration().setInt(
      LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY,
      MAX_FILES_PER_REGION_PER_FAMILY);
    Configuration conf = util.getConfiguration();
    conf.setInt("hbase.rpc.timeout", 15000);
    conf.setInt("hbase.client.retries.number", 8);
    conf.setInt("hbase.client.pause", 800);
    conf.setLong(PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_LOWER_BOUND, 1024);
    conf.setLong( PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_THROUGHPUT_CONTROL_CHECK_INTERVAL, 1024);
	conf.set(FlushThroughputControllerFactory.HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY,
             PressureAwareFlushThroughputController.class.getName());
    util.startMiniCluster();
    setupNamespace();
  }

  protected static void setupNamespace() throws Exception {
    util.getHBaseAdmin().createNamespace(NamespaceDescriptor.create(NAMESPACE).build());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }


  private void putData(String strTable) throws IOException {
	TableName tableName = TableName.valueOf(strTable);
	HBaseAdmin admin = util.getHBaseAdmin();
	if (admin.tableExists(tableName)) {
	  admin.disableTable(tableName);
	  admin.deleteTable(tableName);
	}
	HTable table = util.createTable(tableName, FAMILY);
	Random rand = new Random();
	for (int i = 0; i < 25; i++) {
	  byte[] value = new byte[1024];
	  rand.nextBytes(value);
	  table.put(new Put(Bytes.toBytes(i)).add(FAMILY, QUALIFIER, value));
	}
    admin.close();
  }

  @Test(timeout = 60000)
  public void testFlushTimeoutWhenDoBulkLoad() throws Exception {
    String tableName = new String("testFlushTimeoutWhenDoBulkLoad");
	Configuration conf = util.getConfiguration();
    putData(tableName);

    Path dir = util.getDataTestDirOnTestFS("testFlushTimeoutWhenDoBulkLoad");
    FileSystem fs = util.getTestFileSystem();
    dir = dir.makeQualified(fs);
    Path familyDir = new Path(dir, Bytes.toString(FAMILY));

    byte[] from = Bytes.toBytes("begin");
    byte[] to = Bytes.toBytes("end");
    for (int i = 0; i < 3; i++) {
      HFileTestUtil.createHFile(conf, fs, new Path(familyDir, "hfile_"
          + i), FAMILY, QUALIFIER, from, to, 10);
    }

    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
    String [] args= {dir.toString(), tableName};
    try {
      loader.run(args);
    } catch (FileNotFoundException ie) {
      fail("Should not throw FileNotFoundException and retry when flush timeout!");
    }
  }
}

