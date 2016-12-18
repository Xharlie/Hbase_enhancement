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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestColumnPrefixCountGetFilter {

  private final static HBaseTestingUtility TEST_UTIL = new
      HBaseTestingUtility();

  @Test
  public void testColumnPrefixCountGetFilter() throws IOException {
    String family = "Family";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("TestColumnPrefixCountGetFilter"));
    htd.addFamily((new HColumnDescriptor(family)).setMaxVersions(3));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    HRegion region = HRegion.createHRegion(info, TEST_UTIL.
      getDataTestDir(), TEST_UTIL.getConfiguration(), htd);
    try {
      String[] rows = new String[] {"row01", "row02", "row03"};
      String[] columns = new String[] {"q1", "q12", "q2", "q123", "q3"};
      long maxTimestamp = 2;
      String valueString = "ValueString";
      for (String row: rows) {
        Put p = new Put(Bytes.toBytes(row));
        p.setDurability(Durability.SKIP_WAL);
        for (String column: columns) {
          for (long timestamp = 1; timestamp <= maxTimestamp; timestamp++) {
            KeyValue kv = KeyValueTestUtil.create(row, family, column, timestamp,
                valueString);
            p.add(kv);
          }
        }
        
        region.put(p);
      }

      long totalCells = rows.length * columns.length * maxTimestamp;
      ColumnPrefixCountGetFilter filter = new ColumnPrefixCountGetFilter(Bytes.toBytes("q1"), 1);
      Scan scan = new Scan();
      scan.setMaxVersions();
      scan.setFilter(filter);
      InternalScanner scanner = region.getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      while (scanner.next(results));
      assertEquals(totalCells - rows.length * 2 * maxTimestamp + rows.length, results.size());
      for (Cell kv : results) {
        System.out.println("row: "
            + Bytes.toString(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength())
            + ", column:"
            + Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(),
              kv.getQualifierLength()) + ", ts: " + kv.getTimestamp());
      }
    } finally {
      HRegion.closeHRegion(region);
    }

    HRegion.closeHRegion(region);
  }
  
  @Test
  public void testColumnPrefixFilterWithFilterList() throws IOException {
    String family = "Family";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("TestColumnPrefixCountGetFilter"));
    htd.addFamily((new HColumnDescriptor(family)).setMaxVersions(3));
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    HRegion region = HRegion.createHRegion(info, TEST_UTIL.
      getDataTestDir(), TEST_UTIL.getConfiguration(), htd);
    try {
      String[] rows = new String[] {"row01", "row02", "row03"};
      String[] columns = new String[] {"q1", "q12", "q2", "q123", "q3", "q234"};
      long maxTimestamp = 2;
      String valueString = "ValueString";

      for (String row: rows) {
        Put p = new Put(Bytes.toBytes(row));
        p.setDurability(Durability.SKIP_WAL);
        for (String column: columns) {
          for (long timestamp = 1; timestamp <= maxTimestamp; timestamp++) {
            KeyValue kv = KeyValueTestUtil.create(row, family, column, timestamp,
                valueString);
            p.add(kv);
          }
        }
        region.put(p);
      }

      long totalCells = rows.length * columns.length * maxTimestamp;
      ColumnPrefixCountGetFilter filter1 = new ColumnPrefixCountGetFilter(Bytes.toBytes("q1"), 1);
      ColumnPrefixCountGetFilter filter2 = new ColumnPrefixCountGetFilter(Bytes.toBytes("q2"), 1);
      Scan scan = new Scan();
      scan.setMaxVersions();
      FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
      filterList.addFilter(filter1);
      filterList.addFilter(filter2);
      scan.setFilter(filterList);
      InternalScanner scanner = region.getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      while (scanner.next(results));
      assertEquals(totalCells - rows.length * 3 * maxTimestamp + rows.length * 2, results.size());
      for (Cell kv : results) {
        System.out.println("row: "
            + Bytes.toString(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength())
            + ", column:"
            + Bytes.toString(kv.getQualifierArray(), kv.getQualifierOffset(),
              kv.getQualifierLength()) + ", ts: " + kv.getTimestamp());
      }
    } finally {
      HRegion.closeHRegion(region);
    }

    HRegion.closeHRegion(region);
  }
}

