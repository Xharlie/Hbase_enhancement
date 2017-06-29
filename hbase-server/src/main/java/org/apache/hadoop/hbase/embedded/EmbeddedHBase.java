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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.ConstantFamilySizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.LastSequenceId;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.RegionGroupingProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;

/**
 * Utility class to use HBase in embedded mode
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EmbeddedHBase extends HMaster
    implements RegionServerServices, LastSequenceId, ConfigurationObserver {
  private static final Log LOG = LogFactory.getLog(EmbeddedHBase.class);
  private static MiniZooKeeperCluster zkCluster = null;

  private DBOperator dbOperator;

  public EmbeddedHBase(Configuration conf, CoordinatedStateManager csm)
      throws IOException, InterruptedException, KeeperException {
    super(conf, csm);
  }

  public static EmbeddedHBase open(final Options opts, final String path)
      throws IOException, InterruptedException, KeeperException {
    Configuration conf = HBaseConfiguration.create();
    return open(opts, path, conf);
  }

  @Deprecated
  public static EmbeddedHBase open(final TableName table, final String path)
      throws IOException, InterruptedException, KeeperException {
    Options opts = new Options();
    opts.setTableName(table);
    return open(opts, path);
  }

  @VisibleForTesting
  public static EmbeddedHBase open(final Options opts, final String path, final Configuration conf)
      throws IOException, InterruptedException, KeeperException {
    doNecessaryConfig(conf, path);

    // FIXME now we're still using the zookeeper cluster and assignment manager for prototype, later
    // we should remove all distributed-related stuff

    // initialize mini-zookeeper cluster
    startMiniZookeeperCluster(conf);

    // TODO initialize assignment manager?
    // Get the instance
    CoordinatedStateManager csm = CoordinatedStateManagerFactory.getCoordinatedStateManager(conf);
    EmbeddedHBase eHBase = new EmbeddedHBase(conf, csm);
    eHBase.start();
    waitUntilStarted(eHBase);
    eHBase.setDBOprator(createTableIfNotExist(eHBase, opts));
    return eHBase;
  }

  /**
   * Necessary hard-coded configurations like using embedded mode, disable replication, etc.
   * @param conf The configuration instance
   * @param path The given path for the embedded database
   */
  private static void doNecessaryConfig(Configuration conf, String path) {
    conf.set(HConstants.HBASE_DIR, path);
    conf.setBoolean(HConstants.EMBEDDED_MODE, true);
    //conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, false);

    conf.set(HConstants.ZOOKEEPER_DATA_DIR, path + File.separator + "zookeeper");
    // set no table on master so we won't require 2 RS
    conf.set(BaseLoadBalancer.TABLES_ON_MASTER, "none");
    // set server handler count although no rpc server
    // since FSHLog sync queue length is based on this setting
    conf.setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 500);
    // increase blocking store files number to some higher value
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 20);
    // ConstantFamilySizeRegionSplitPolicy is necessary to disable split
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
        ConstantFamilySizeRegionSplitPolicy.class.getName());
    // disable mvcc preassign for embedded mode
    //conf.setBoolean(HRegion.HREGION_MVCC_PRE_ASSIGN, false);
    // use one WAL per region to increase writing throughput
    conf.set(WALFactory.WAL_PROVIDER, "multiwal");
    //conf.set(RegionGroupingProvider.REGION_GROUPING_STRATEGY, "identity");
    conf.set(RegionGroupingProvider.REGION_GROUPING_STRATEGY, "identity");
    // enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
  }

  private static Table createTableIfNotExist(EmbeddedHBase eHBase, Options opts)
      throws IOException {
    TableName tableName = opts.getTableName();
    if (tableName == null) {
      throw new IllegalArgumentException("Table name not set");
    }
    ClusterConnection conn = eHBase.getConnection();
    Admin admin = conn.getAdmin();
    if (!admin.tableExists(tableName)) {
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      tableDesc.setDurability(opts.getDurability());
      addFamilies(tableDesc, opts);
      tableDesc.setMaxFileSize(Long.MAX_VALUE);
      byte[][] splitKeys = opts.getSplitKeys();
      eHBase.createTable(tableDesc, splitKeys);
    }
    while (!admin.isTableAvailable(tableName)) {
      Threads.sleep(1000);
    }
    return conn.getTable(tableName);
  }

  private static void addFamilies(HTableDescriptor tableDesc, Options opts) {
    if (opts.getFamilies() == null) {
      HColumnDescriptor family = new HColumnDescriptor(Options.DEFAULT_FAMILY);
      setFamily(family, opts);
      tableDesc.addFamily(family);
      return;
    }
    for (byte[] familyName : opts.getFamilies()) {
      HColumnDescriptor family = new HColumnDescriptor(familyName);
      setFamily(family, opts);
      tableDesc.addFamily(family);
    }
  }

  private static void setFamily(HColumnDescriptor family, Options opts) {
    family.setCompressionType(opts.getCompression());
    family.setBlocksize(opts.getBlockSize());
  }

  static byte[][] getSplitKeys(int splits) {
    byte[][] splitKeys = new byte[splits][];
    for (int i = 0; i < splits; i++) {
      String key = "user" + (1000 + (i + 1) * (9999 - 1000) / splits);
      LOG.trace("key of split " + i + ": " + key);
      splitKeys[i] = Bytes.toBytes(key);
    }
    return splitKeys;
  }

  private static void waitUntilStarted(EmbeddedHBase eHBase) {
    while (!eHBase.isInitialized()) {
      Threads.sleep(1000);
    }
  }

  private void setDBOprator(Table table) {
    String dbOperatorClass =
        conf.get(HConstants.EMBEDDED_DB_OPERATOR_CLASS, DirectDBOperator.class.getName());
    try {
      this.dbOperator =
          (DBOperator) ReflectionUtils.newInstance(Class.forName(dbOperatorClass), this, table);
      LOG.debug("DB operator: " + dbOperator.getClass().getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static void startMiniZookeeperCluster(Configuration conf)
      throws IOException, InterruptedException {
    final MiniZooKeeperCluster zooKeeperCluster = new MiniZooKeeperCluster(conf);
    File zkDataPath = new File(conf.get(HConstants.ZOOKEEPER_DATA_DIR));
    LOG.debug("zookeeper path: "+zkDataPath.getAbsolutePath());

    // find out the default client port
    int zkClientPort = 0;

    // If the zookeeper client port is specified in server quorum, use it.
    String zkserver = conf.get(HConstants.ZOOKEEPER_QUORUM);
    if (zkserver != null) {
      String[] zkservers = zkserver.split(",");

      if (zkservers.length > 1) {
        // In embedded mode we only support one zookeeper server.
        String errorMsg =
            "Could not start ZK with " + zkservers.length + " ZK servers in embedded mode.";
        LOG.error(errorMsg);
        throw new IOException(errorMsg);
      }

      String[] parts = zkservers[0].split(":");

      if (parts.length == 2) {
        // the second part is the client port
        zkClientPort = Integer.parseInt(parts[1]);
      }
    }
    // If the client port could not be find in server quorum conf, try another conf
    if (zkClientPort == 0) {
      zkClientPort = conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 0);
      // The client port has to be set by now; if not, throw exception.
      if (zkClientPort == 0) {
        throw new IOException("No config value for " + HConstants.ZOOKEEPER_CLIENT_PORT);
      }
    }
    zooKeeperCluster.setDefaultClientPort(zkClientPort);
    // set the ZK tick time if specified
    int zkTickTime = conf.getInt(HConstants.ZOOKEEPER_TICK_TIME, 0);
    if (zkTickTime > 0) {
      zooKeeperCluster.setTickTime(zkTickTime);
    }

    // login the zookeeper server principal (if using security)
    ZKUtil.loginServer(conf, "hbase.zookeeper.server.keytab.file",
        "hbase.zookeeper.server.kerberos.principal", null);
    int localZKClusterSessionTimeout =
        conf.getInt(HConstants.ZK_SESSION_TIMEOUT + ".localHBaseCluster", 10 * 1000);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, localZKClusterSessionTimeout);
    LOG.info("Starting a zookeeper cluster");
    int clientPort = zooKeeperCluster.startup(zkDataPath);
    if (clientPort != zkClientPort) {
      String msg = "Could not start ZK at requested port of " + zkClientPort
          + ". Instead ZK is started at port: " + clientPort;
      LOG.warn(msg);
    }
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));
    zkCluster = zooKeeperCluster;
  }

  @Override
  protected RSRpcServices createRpcServices() throws IOException {
    return new EmbeddedRpcServices(this);
  }

  public void put(final Put put) throws IOException {
    dbOperator.put(put);
  }

  public Result get(final Get get) throws IOException {
    return dbOperator.get(get);
  }

  public Result append(final Append append) throws IOException {
    return dbOperator.append(append);
  }

  public Result increment(final Increment increment) throws IOException {
    return dbOperator.increment(increment);
  }

  public void delete(final Delete delete) throws IOException {
    dbOperator.delete(delete);
  }

  public ResultScanner getResultScanner(final Scan scan) throws IOException {
    return dbOperator.getScanner(scan);
  }

  public void snapshot(final String snapshotName) throws IOException {
    dbOperator.snapshot(snapshotName);
  }

  public void deleteSnapshot(final String snapshotName) throws IOException {
    dbOperator.deleteSnapshot(snapshotName);
  }

  public void restoreSnapshot(final String snapshotName) throws IOException {
    TableName table = dbOperator.getName();
    // disable table first before restore
    Admin admin = getConnection().getAdmin();
    admin.disableTable(table);
    dbOperator.restoreSnapshot(snapshotName, table.getNameAsString());
    // enable table after restore
    admin.enableTable(table);
    while (!admin.isTableAvailable(table)) {
      Threads.sleep(1000);
    }
  }

  public void close() throws IOException {
    this.shutdown();
    waitUntilStopped();
    if (zkCluster != null) {
      zkCluster.shutdown();
    }
  }

  private void waitUntilStopped() {
    while (!isShutDown()) {
      Threads.sleep(1000);
    }
  }

  public static void main(String[] args) {
    TableName table = TableName.valueOf("test");
    EmbeddedHBase eHBase = null;
    try {
      eHBase = EmbeddedHBase.open(table, "/tmp/embeddedhbase");
    } catch (Exception e) {
      LOG.fatal("Failed to open database", e);
      throw new RuntimeException(e);
    }
    LOG.debug("Server started");
    final byte[] row = Bytes.toBytes("row");
    final byte[] family = Bytes.toBytes("cf");
    final byte[] column = Bytes.toBytes("c1");
    final byte[] value = Bytes.toBytes("value");
    Put put = new Put(row);
    put.addColumn(family, column, value);
    try {
      eHBase.put(put);
    } catch (IOException e) {
      LOG.error("Failed to put into database", e);
      try {
        eHBase.close();
      } catch (IOException ex) {
      }
      throw new RuntimeException(e);
    }
    Get get = new Get(row);
    Result result = null;
    try {
      result = eHBase.get(get);
      LOG.debug("Result: " + Bytes.toString(result.getValue(family, column)));
    } catch (IOException e) {
      LOG.error("Failed to get data from database", e);
      try {
        eHBase.close();
      } catch (IOException ex) {
      }
      throw new RuntimeException(e);
    }
    LOG.debug("Stopping server");
    if(eHBase!=null){
      try {
        eHBase.close();
      } catch (IOException e) {
        LOG.warn("Failed to stop server", e);
      }
    }
  }
}
