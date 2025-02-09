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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * Class for testing WALObserver coprocessor.
 *
 * It will monitor WAL writing and restoring, and modify passed-in WALEdit, i.e,
 * ignore specified columns when writing, or add a KeyValue. On the other
 * side, it checks whether the ignored column is still in WAL when Restoreed
 * at region reconstruct.
 */
public class SampleRegionWALObserver extends BaseRegionObserver
implements WALObserver {

  private static final Log LOG = LogFactory.getLog(SampleRegionWALObserver.class);

  private byte[] tableName;
  private byte[] row;
  private byte[] ignoredFamily;
  private byte[] ignoredQualifier;
  private byte[] addedFamily;
  private byte[] addedQualifier;
  private byte[] changedFamily;
  private byte[] changedQualifier;

  private boolean preWALWriteCalled = false;
  private boolean postWALWriteCalled = false;
  private boolean preWALRestoreCalled = false;
  private boolean postWALRestoreCalled = false;

  // Deprecated versions
  private boolean preWALWriteDeprecatedCalled = false;
  private boolean postWALWriteDeprecatedCalled = false;
  private boolean preWALRestoreDeprecatedCalled = false;
  private boolean postWALRestoreDeprecatedCalled = false;

  /**
   * Set values: with a table name, a column name which will be ignored, and
   * a column name which will be added to WAL.
   */
  public void setTestValues(byte[] tableName, byte[] row, byte[] igf, byte[] igq,
      byte[] chf, byte[] chq, byte[] addf, byte[] addq) {
    this.row = row;
    this.tableName = tableName;
    this.ignoredFamily = igf;
    this.ignoredQualifier = igq;
    this.addedFamily = addf;
    this.addedQualifier = addq;
    this.changedFamily = chf;
    this.changedQualifier = chq;
    preWALWriteCalled = false;
    postWALWriteCalled = false;
    preWALRestoreCalled = false;
    postWALRestoreCalled = false;
    preWALWriteDeprecatedCalled = false;
    postWALWriteDeprecatedCalled = false;
    preWALRestoreDeprecatedCalled = false;
    postWALRestoreDeprecatedCalled = false;
  }

  @Override
  public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> env,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    postWALWriteCalled = true;
  }

  @Override
  public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    postWALWriteDeprecatedCalled = true;
    postWALWrite(env, info, (WALKey)logKey, logEdit);
  }

  @Override
  public boolean preWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> env,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    boolean bypass = false;
    // check table name matches or not.
    if (!Bytes.equals(info.getTableName(), this.tableName)) {
      return bypass;
    }
    preWALWriteCalled = true;
    // here we're going to remove one keyvalue from the WALEdit, and add
    // another one to it.
    List<Cell> cells = logEdit.getCells();
    Cell deletedCell = null;
    for (Cell cell : cells) {
      // assume only one kv from the WALEdit matches.
      byte[] family = CellUtil.cloneFamily(cell);
      byte[] qulifier = CellUtil.cloneQualifier(cell);

      if (Arrays.equals(family, ignoredFamily) &&
          Arrays.equals(qulifier, ignoredQualifier)) {
        LOG.debug("Found the KeyValue from WALEdit which should be ignored.");
        deletedCell = cell;
      }
      if (Arrays.equals(family, changedFamily) &&
          Arrays.equals(qulifier, changedQualifier)) {
        LOG.debug("Found the KeyValue from WALEdit which should be changed.");
        cell.getValueArray()[cell.getValueOffset()] += 1;
      }
    }
    if (null != row) {
      cells.add(new KeyValue(row, addedFamily, addedQualifier));
    }
    if (deletedCell != null) {
      LOG.debug("About to delete a KeyValue from WALEdit.");
      cells.remove(deletedCell);
    }
    return bypass;
  }

  @Override
  public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    preWALWriteDeprecatedCalled = true;
    return preWALWrite(env, info, (WALKey)logKey, logEdit);
  }

  /**
   * Triggered before  {@link org.apache.hadoop.hbase.regionserver.HRegion} when WAL is
   * Restoreed.
   */
  @Override
  public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    preWALRestoreCalled = true;
  }

  @Override
  public void preWALRestore(ObserverContext<RegionCoprocessorEnvironment> env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    preWALRestoreDeprecatedCalled = true;
    preWALRestore(env, info, (WALKey)logKey, logEdit);
  }

  /**
   * Triggered after {@link org.apache.hadoop.hbase.regionserver.HRegion} when WAL is
   * Restoreed.
   */
  @Override
  public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env,
      HRegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    postWALRestoreCalled = true;
  }

  @Override
  public void postWALRestore(ObserverContext<RegionCoprocessorEnvironment> env,
      HRegionInfo info, HLogKey logKey, WALEdit logEdit) throws IOException {
    postWALRestoreDeprecatedCalled = true;
    postWALRestore(env, info, (WALKey)logKey, logEdit);
  }

  public boolean isPreWALWriteCalled() {
    return preWALWriteCalled;
  }

  public boolean isPostWALWriteCalled() {
    return postWALWriteCalled;
  }

  public boolean isPreWALRestoreCalled() {
    LOG.debug(SampleRegionWALObserver.class.getName() +
      ".isPreWALRestoreCalled is called.");
    return preWALRestoreCalled;
  }

  public boolean isPostWALRestoreCalled() {
    LOG.debug(SampleRegionWALObserver.class.getName() +
      ".isPostWALRestoreCalled is called.");
    return postWALRestoreCalled;
  }

  public boolean isPreWALWriteDeprecatedCalled() {
    return preWALWriteDeprecatedCalled;
  }

  public boolean isPostWALWriteDeprecatedCalled() {
    return postWALWriteDeprecatedCalled;
  }

  public boolean isPreWALRestoreDeprecatedCalled() {
    return preWALRestoreDeprecatedCalled;
  }

  public boolean isPostWALRestoreDeprecatedCalled() {
    return postWALRestoreDeprecatedCalled;
  }

  /**
   * This class should trigger our legacy support since it does not directly implement the
   * newer API methods.
   */
  static class Legacy extends SampleRegionWALObserver {
  }
}
