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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InterruptedIOException;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EmbeddedRpcServices extends MasterRpcServices {
  private static final Log LOG = LogFactory.getLog(EmbeddedRpcServices.class);
  private final HMaster server;
  private final long pause;
  private final int numRetries;

  public EmbeddedRpcServices(HMaster server) throws IOException {
    super(server);
    this.server = server;
    this.pause = server.getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numRetries = server.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
  }

  @Override
  protected void start() {
    // do nothing here
  }

  @Override
  protected void stop() {
    closeAllScanners();
  }

  public Result get(TableName tableName, Get get) throws IOException {
    checkOpen();
    Region region = getRegion(tableName, get.getRow());
    return get(get, ((HRegion) region));
  }

  private Result get(Get get, HRegion region) throws IOException {
    return region.get(get);
  }

  public void put(TableName tableName, Put put) throws IOException {
    getRegion(tableName, put).put(put);
  }

  public void delete(TableName tableName, Delete delete) throws IOException {
    getRegion(tableName, delete).delete(delete);
  }

  public Result append(TableName tableName, Append append) throws IOException {
    // don't need nonce in embedded mode, see #HBASE-3787 about nonce
    return getRegion(tableName, append).append(append, HConstants.NO_NONCE, HConstants.NO_NONCE);
  }

  public Result increment(TableName tableName, Increment increment) throws IOException {
    // don't need nonce in embedded mode, see #HBASE-3787 about nonce
    return getRegion(tableName, increment).increment(increment, HConstants.NO_NONCE,
        HConstants.NO_NONCE);
  }

  /**
   * Get region for mutation, and reclaim memstore memory if necessary
   * @param tableName the name of the table
   * @param mutate the mutation
   * @return the region to operate
   * @throws IOException if any error occurs
   */
  private Region getRegion(TableName tableName, Mutation mutate) throws IOException {
    checkOpen();
    Region region = getRegion(tableName, mutate.getRow());
    if (!region.getRegionInfo().isMetaTable()) {
      reclaimMemStoreMemory();
    }
    return region;
  }

  public ResultScanner getScanner(TableName tableName, Scan scan) throws IOException {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(scan);

    checkOpen();

    if (!scan.isReversed()) {
      return new EmbeddedSimpleScanner(server.getConfiguration(), scan, tableName, server);
    } else {
      return new EmbeddedReverseScanner(server.getConfiguration(), scan, tableName, server);
    }
  }

  @Override
  public ClientProtos.ScanResponse scan(final RpcController controller, final ClientProtos.ScanRequest request)
      throws ServiceException {
    return super.scan(controller, request);
  }

  private Region getRegion(TableName tableName, byte[] row) throws IOException {
    byte[] regionName =
        server.getConnection().locateRegion(tableName, row).getRegionInfo().getRegionName();
    return server.getRegionByEncodedName(HRegionInfo.encodeRegionName(regionName));
  }

  public void snapshot(final String snapshotName, final TableName tableName) throws IOException {
    snapshot(snapshotName, tableName, HBaseProtos.SnapshotDescription.Type.FLUSH);
  }

  public void snapshot(final String snapshotName, final TableName tableName, HBaseProtos.SnapshotDescription.Type type)
      throws IOException {
    HBaseProtos.SnapshotDescription.Builder builder = HBaseProtos.SnapshotDescription.newBuilder();
    builder.setTable(tableName.getNameAsString());
    builder.setName(snapshotName);
    builder.setType(type);
    HBaseProtos.SnapshotDescription snapshot = builder.build();
    snapshot = SnapshotDescriptionUtils.validate(snapshot, server.getConfiguration());
    SnapshotManager snapshotManager = server.getSnapshotManagerForTesting();
    LOG.info("Snapshot request for:" + ClientSnapshotDescriptionUtils.toString(snapshot));
    snapshotManager.takeSnapshot(snapshot);
    long max = SnapshotDescriptionUtils.getMaxMasterTimeout(server.getConfiguration(), snapshot.getType(),
        SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME);
    long start = EnvironmentEdgeManager.currentTime();
    long maxPauseTime = max / this.numRetries;
    int tries = 0;
    boolean done = false;
    while (tries == 0 || ((EnvironmentEdgeManager.currentTime() - start) < max && !done)) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug(
            "(#" + tries + ") Sleeping: " + sleep + "ms while waiting for snapshot completion.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        throw (InterruptedIOException) new InterruptedIOException("Interrupted").initCause(e);
      }
      LOG.debug("Getting current status of snapshot from master...");
      done = snapshotManager.isSnapshotDone(snapshot);
    }
    if (!done) {
      throw new SnapshotCreationException(
          "Snapshot '" + snapshot.getName() + "' wasn't completed in expectedTime:" + max + " ms",
          snapshot);
    }
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length) {
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    }
    return this.pause * HConstants.RETRY_BACKOFF[triesCount];
  }

  public void deleteSnapshot(final String snapshotName) throws IOException {
    HBaseProtos.SnapshotDescription snapshot =
        HBaseProtos.SnapshotDescription.newBuilder().setName(snapshotName).build();
    LOG.info("Delete snapshot: " + snapshot.getName());
    server.getSnapshotManagerForTesting().deleteSnapshot(snapshot);
  }

  public void restoreSnapshot(final String snapshotName, final String tableName)
      throws IOException {
    final HBaseProtos.SnapshotDescription snapshot = HBaseProtos.SnapshotDescription.newBuilder()
        .setName(snapshotName).setTable(tableName).build();
    LOG.info("Restore snapshot: " + snapshot.getName());
    server.getSnapshotManagerForTesting().restoreSnapshot(snapshot);
  }
}
