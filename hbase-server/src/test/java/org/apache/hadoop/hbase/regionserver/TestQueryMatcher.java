/*
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

package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode.INCLUDE;
import static org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode.SKIP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.experimental.categories.Category;
import org.junit.Before;
import org.junit.Test;

@Category(SmallTests.class)
public class TestQueryMatcher {
  private static final boolean PRINT = false;
  private Configuration conf;

  private byte[] row1;
  private byte[] row2;
  private byte[] row3;
  private byte[] fam1;
  private byte[] fam2;
  private byte[] col1;
  private byte[] col2;
  private byte[] col3;
  private byte[] col4;
  private byte[] col5;

  private byte[] data;

  private Get get;

  long ttl = Long.MAX_VALUE;
  CellComparator rowComparator;
  private Scan scan;

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create();
    row1 = Bytes.toBytes("row1");
    row2 = Bytes.toBytes("row2");
    row3 = Bytes.toBytes("row3");
    fam1 = Bytes.toBytes("fam1");
    fam2 = Bytes.toBytes("fam2");
    col1 = Bytes.toBytes("col1");
    col2 = Bytes.toBytes("col2");
    col3 = Bytes.toBytes("col3");
    col4 = Bytes.toBytes("col4");
    col5 = Bytes.toBytes("col5");

    data = Bytes.toBytes("data");

    //Create Get
    get = new Get(row1);
    get.addFamily(fam1);
    get.addColumn(fam2, col2);
    get.addColumn(fam2, col4);
    get.addColumn(fam2, col5);
    this.scan = new Scan(get);

    rowComparator = CellComparator.COMPARATOR;

  }

  /**
  * This is a cryptic test. It is checking that we don't include a fake cell, one that has a
  * timestamp of {@link HConstants#OLDEST_TIMESTAMP}. See HBASE-16074 for background.
  * @throws IOException
  */
  @Test
  public void testNeverIncludeFakeCell() throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    // Do with fam2 which has a col2 qualifier.
    ScanQueryMatcher qm = new ScanQueryMatcher(scan,
        new ScanInfo(this.conf, fam2, 10, 1, ttl, KeepDeletedCells.FALSE, 0, rowComparator),
        get.getFamilyMap().get(fam2), now - ttl, now);
    Cell kv = new KeyValue(row1, fam2, col2, 1, data);
    Cell cell = CellUtil.createLastOnRowCol(kv);
    qm.setToNewRow(kv);
    MatchCode code = qm.match(cell);
    assertFalse(code.compareTo(MatchCode.SEEK_NEXT_COL) != 0);
  }

  private void _testMatch_ExplicitColumns(Scan scan, List<MatchCode> expected) throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    // 2,4,5
    ScanQueryMatcher qm = new ScanQueryMatcher(scan, new ScanInfo(this.conf, fam2,
        0, 1, ttl, KeepDeletedCells.FALSE, 0, rowComparator), get.getFamilyMap().get(fam2),
        now - ttl, now);

    List<KeyValue> memstore = new ArrayList<KeyValue>();
    memstore.add(new KeyValue(row1, fam2, col1, 1, data));
    memstore.add(new KeyValue(row1, fam2, col2, 1, data));
    memstore.add(new KeyValue(row1, fam2, col3, 1, data));
    memstore.add(new KeyValue(row1, fam2, col4, 1, data));
    memstore.add(new KeyValue(row1, fam2, col5, 1, data));

    memstore.add(new KeyValue(row2, fam1, col1, data));

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<ScanQueryMatcher.MatchCode>();
    KeyValue k = memstore.get(0);
    qm.setToNewRow(k);

    for (KeyValue kv : memstore){
      actual.add(qm.match(kv));
    }

    assertEquals(expected.size(), actual.size());
    for(int i=0; i< expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
      if(PRINT){
        System.out.println("expected "+expected.get(i)+
            ", actual " +actual.get(i));
      }
    }
  }

  @Test
  public void testMatch_ExplicitColumns()
  throws IOException {
    //Moving up from the Tracker by using Gets and List<KeyValue> instead
    //of just byte []

    //Expected result
    List<MatchCode> expected = new ArrayList<ScanQueryMatcher.MatchCode>();
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW);
    expected.add(ScanQueryMatcher.MatchCode.DONE);

    _testMatch_ExplicitColumns(scan, expected);
  }

  @Test
  public void testMatch_Wildcard()
  throws IOException {
    //Moving up from the Tracker by using Gets and List<KeyValue> instead
    //of just byte []

    //Expected result
    List<MatchCode> expected = new ArrayList<ScanQueryMatcher.MatchCode>();
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.INCLUDE);
    expected.add(ScanQueryMatcher.MatchCode.DONE);

    long now = EnvironmentEdgeManager.currentTime();
    ScanQueryMatcher qm = new ScanQueryMatcher(scan, new ScanInfo(this.conf, fam2,
        0, 1, ttl, KeepDeletedCells.FALSE, 0, rowComparator), null,
        now - ttl, now);

    List<KeyValue> memstore = new ArrayList<KeyValue>();
    memstore.add(new KeyValue(row1, fam2, col1, 1, data));
    memstore.add(new KeyValue(row1, fam2, col2, 1, data));
    memstore.add(new KeyValue(row1, fam2, col3, 1, data));
    memstore.add(new KeyValue(row1, fam2, col4, 1, data));
    memstore.add(new KeyValue(row1, fam2, col5, 1, data));
    memstore.add(new KeyValue(row2, fam1, col1, 1, data));

    List<ScanQueryMatcher.MatchCode> actual = new ArrayList<ScanQueryMatcher.MatchCode>();

    KeyValue k = memstore.get(0);
    qm.setToNewRow(k);

    for(KeyValue kv : memstore) {
      actual.add(qm.match(kv));
    }

    assertEquals(expected.size(), actual.size());
    for(int i=0; i< expected.size(); i++){
      assertEquals(expected.get(i), actual.get(i));
      if(PRINT){
        System.out.println("expected "+expected.get(i)+
            ", actual " +actual.get(i));
      }
    }
  }


  /**
   * Verify that {@link ScanQueryMatcher} only skips expired KeyValue
   * instances and does not exit early from the row (skipping
   * later non-expired KeyValues).  This version mimics a Get with
   * explicitly specified column qualifiers.
   *
   * @throws IOException
   */
  @Test
  public void testMatch_ExpiredExplicit()
  throws IOException {

    long testTTL = 1000;
    MatchCode [] expected = new MatchCode[] {
        ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.SEEK_NEXT_ROW,
        ScanQueryMatcher.MatchCode.DONE
    };

    long now = EnvironmentEdgeManager.currentTime();
    ScanQueryMatcher qm =
      new ScanQueryMatcher(scan,
        new ScanInfo(this.conf, fam2, 0, 1, testTTL, KeepDeletedCells.FALSE, 0,
          rowComparator), get.getFamilyMap().get(fam2), now - testTTL, now);

    KeyValue [] kvs = new KeyValue[] {
        new KeyValue(row1, fam2, col1, now-100, data),
        new KeyValue(row1, fam2, col2, now-50, data),
        new KeyValue(row1, fam2, col3, now-5000, data),
        new KeyValue(row1, fam2, col4, now-500, data),
        new KeyValue(row1, fam2, col5, now-10000, data),
        new KeyValue(row2, fam1, col1, now-10, data)
    };

    KeyValue k = kvs[0];
    qm.setToNewRow(k);

    List<MatchCode> actual = new ArrayList<MatchCode>(kvs.length);
    for (KeyValue kv : kvs) {
      actual.add( qm.match(kv) );
    }

    assertEquals(expected.length, actual.size());
    for (int i=0; i<expected.length; i++) {
      if(PRINT){
        System.out.println("expected "+expected[i]+
            ", actual " +actual.get(i));
      }
      assertEquals(expected[i], actual.get(i));
    }
  }


  /**
   * Verify that {@link ScanQueryMatcher} only skips expired KeyValue
   * instances and does not exit early from the row (skipping
   * later non-expired KeyValues).  This version mimics a Get with
   * wildcard-inferred column qualifiers.
   *
   * @throws IOException
   */
   @Test
  public void testMatch_ExpiredWildcard()
  throws IOException {

    long testTTL = 1000;
    MatchCode [] expected = new MatchCode[] {
        ScanQueryMatcher.MatchCode.INCLUDE,
        ScanQueryMatcher.MatchCode.INCLUDE,
        ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.INCLUDE,
        ScanQueryMatcher.MatchCode.SEEK_NEXT_COL,
        ScanQueryMatcher.MatchCode.DONE
    };

    long now = EnvironmentEdgeManager.currentTime();
    ScanQueryMatcher qm = new ScanQueryMatcher(scan, new ScanInfo(this.conf, fam2,
        0, 1, testTTL, KeepDeletedCells.FALSE, 0, rowComparator), null,
        now - testTTL, now);

    KeyValue [] kvs = new KeyValue[] {
        new KeyValue(row1, fam2, col1, now-100, data),
        new KeyValue(row1, fam2, col2, now-50, data),
        new KeyValue(row1, fam2, col3, now-5000, data),
        new KeyValue(row1, fam2, col4, now-500, data),
        new KeyValue(row1, fam2, col5, now-10000, data),
        new KeyValue(row2, fam1, col1, now-10, data)
    };
    KeyValue k = kvs[0];
    qm.setToNewRow(k);

    List<ScanQueryMatcher.MatchCode> actual =
        new ArrayList<ScanQueryMatcher.MatchCode>(kvs.length);
    for (KeyValue kv : kvs) {
      actual.add( qm.match(kv) );
    }

    assertEquals(expected.length, actual.size());
    for (int i=0; i<expected.length; i++) {
      if(PRINT){
        System.out.println("expected "+expected[i]+
            ", actual " +actual.get(i));
      }
      assertEquals(expected[i], actual.get(i));
    }
  }

   @Test
  public void testMatch_PartialRangeDropDeletes() throws Exception {
    // Some ranges.
    testDropDeletes(
        row2, row3, new byte[][] { row1, row2, row2, row3 }, INCLUDE, SKIP, SKIP, INCLUDE);
    testDropDeletes(row2, row3, new byte[][] { row1, row1, row2 }, INCLUDE, INCLUDE, SKIP);
    testDropDeletes(row2, row3, new byte[][] { row2, row3, row3 }, SKIP, INCLUDE, INCLUDE);
    testDropDeletes(row1, row3, new byte[][] { row1, row2, row3 }, SKIP, SKIP, INCLUDE);
    // Open ranges.
    testDropDeletes(HConstants.EMPTY_START_ROW, row3,
        new byte[][] { row1, row2, row3 }, SKIP, SKIP, INCLUDE);
    testDropDeletes(row2, HConstants.EMPTY_END_ROW,
        new byte[][] { row1, row2, row3 }, INCLUDE, SKIP, SKIP);
    testDropDeletes(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
        new byte[][] { row1, row2, row3, row3 }, SKIP, SKIP, SKIP, SKIP);

    // No KVs in range.
    testDropDeletes(row2, row3, new byte[][] { row1, row1, row3 }, INCLUDE, INCLUDE, INCLUDE);
    testDropDeletes(row2, row3, new byte[][] { row3, row3 }, INCLUDE, INCLUDE);
    testDropDeletes(row2, row3, new byte[][] { row1, row1 }, INCLUDE, INCLUDE);
  }

  private void testDropDeletes(
      byte[] from, byte[] to, byte[][] rows, MatchCode... expected) throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    // Set time to purge deletes to negative value to avoid it ever happening.
    ScanInfo scanInfo =
      new ScanInfo(this.conf, fam2, 0, 1, ttl, KeepDeletedCells.FALSE, -1L, rowComparator);
    NavigableSet<byte[]> cols = get.getFamilyMap().get(fam2);

    ScanQueryMatcher qm = new ScanQueryMatcher(scan, scanInfo, cols, Long.MAX_VALUE,
        HConstants.OLDEST_TIMESTAMP, HConstants.OLDEST_TIMESTAMP, now, from, to, null);
    List<ScanQueryMatcher.MatchCode> actual =
        new ArrayList<ScanQueryMatcher.MatchCode>(rows.length);
    byte[] prevRow = null;
    for (byte[] row : rows) {
      if (prevRow == null || !Bytes.equals(prevRow, row)) {
        qm.setToNewRow(KeyValueUtil.createFirstOnRow(row));
        prevRow = row;
      }
      actual.add(qm.match(new KeyValue(row, fam2, null, now, Type.Delete)));
    }

    assertEquals(expected.length, actual.size());
    for (int i = 0; i < expected.length; i++) {
      if (PRINT) System.out.println("expected " + expected[i] + ", actual " + actual.get(i));
      assertEquals(expected[i], actual.get(i));
    }
  }
}

