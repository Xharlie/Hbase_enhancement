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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;

import com.google.protobuf.RpcCallback;

/** Internal methods on HConnection that should not be used by user code. */
@InterfaceAudience.Private
// NOTE: Although this class is public, this class is meant to be used directly from internal
// classes and unit tests only.
public interface ClusterConnection extends HConnection {

  /** @return - true if the master server is running
   * @deprecated this has been deprecated without a replacement */
  @Override
  @Deprecated
  boolean isMasterRunning()
      throws MasterNotRunningException, ZooKeeperConnectionException;

  /**
   * Use this api to check if the table has been created with the specified number of
   * splitkeys which was used while creating the given table.
   * Note : If this api is used after a table's region gets splitted, the api may return
   * false.
   * @param tableName
   *          tableName
   * @param splitKeys
   *          splitKeys used while creating table
   * @throws IOException
   *           if a remote or network exception occurs
   */
  @Override
  boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws
      IOException;

  /**
   * Find the location of the region of <i>tableName</i> that <i>row</i>
   * lives in.
   * @param tableName name of the table <i>row</i> is in
   * @param row row key you're trying to find the region of
   * @return HRegionLocation that describes where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  public HRegionLocation locateRegion(final TableName tableName,
      final byte [] row) throws IOException;

  /**
   * Allows flushing the region cache.
   */
  @Override
  void clearRegionCache();

  /**
   * Allows flushing the region cache of all locations that pertain to
   * <code>tableName</code>
   * @param tableName Name of the table whose regions we are to remove from
   * cache.
   */
  @Override
  void clearRegionCache(final TableName tableName);

  /**
   * Deletes cached locations for the specific region.
   * @param location The location object for the region, to be purged from cache.
   */
  @Override
  void deleteCachedRegionLocation(final HRegionLocation location);

  /**
   * Find the location of the region of <i>tableName</i> that <i>row</i>
   * lives in, ignoring any value that might be in the cache.
   * @param tableName name of the table <i>row</i> is in
   * @param row row key you're trying to find the region of
   * @return HRegionLocation that describes where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  HRegionLocation relocateRegion(final TableName tableName,
      final byte [] row) throws IOException;

  /**
   * Find the location of the region of <i>tableName</i> that <i>row</i>
   * lives in, ignoring any value that might be in the cache.
   * @param tableName name of the table <i>row</i> is in
   * @param row row key you're trying to find the region of
   * @param replicaId the replicaId of the region
   * @return RegionLocations that describe where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   */
  RegionLocations relocateRegion(final TableName tableName,
      final byte [] row, int replicaId) throws IOException;

  /**
   * Update the location cache. This is used internally by HBase, in most cases it should not be
   *  used by the client application.
   * @param tableName the table name
   * @param regionName the region name
   * @param rowkey the row
   * @param exception the exception if any. Can be null.
   * @param source the previous location
   */
  @Override
  void updateCachedLocations(TableName tableName, byte[] regionName, byte[] rowkey,
                                    Object exception, ServerName source);


  /**
   * Gets the location of the region of <i>regionName</i>.
   * @param regionName name of the region to locate
   * @return HRegionLocation that describes where to find the region in
   * question
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  HRegionLocation locateRegion(final byte[] regionName)
  throws IOException;

  /**
   * Gets the locations of all regions in the specified table, <i>tableName</i>.
   * @param tableName table to get regions of
   * @return list of region locations for all regions of table
   * @throws IOException
   */
  @Override
  List<HRegionLocation> locateRegions(final TableName tableName) throws IOException;

  /**
   * Gets the locations of all regions in the specified table, <i>tableName</i>.
   * @param tableName table to get regions of
   * @param useCache Should we use the cache to retrieve the region information.
   * @param offlined True if we are to include offlined regions, false and we'll leave out offlined
   *          regions from returned list.
   * @return list of region locations for all regions of table
   * @throws IOException
   */
  @Override
  List<HRegionLocation> locateRegions(final TableName tableName,
      final boolean useCache,
      final boolean offlined) throws IOException;

  /**
   *
   * @param tableName table to get regions of
   * @param row the row
   * @param useCache Should we use the cache to retrieve the region information.
   * @param retry do we retry
   * @return region locations for this row.
   * @throws IOException
   */
  RegionLocations locateRegion(TableName tableName,
                               byte[] row, boolean useCache, boolean retry) throws IOException;

  /**
  *
  * @param tableName table to get regions of
  * @param row the row
  * @param useCache Should we use the cache to retrieve the region information.
  * @param retry do we retry
  * @param replicaId the replicaId for the region
  * @return region locations for this row.
  * @throws IOException
  */
 RegionLocations locateRegion(TableName tableName,
                              byte[] row, boolean useCache, boolean retry, int replicaId) throws IOException;

  /**
   * Returns a {@link MasterKeepAliveConnection} to the active master
   */
  @Override
  MasterService.BlockingInterface getMaster() throws IOException;


  /**
   * Establishes a connection to the region server at the specified address.
   * @param serverName
   * @return proxy for HRegionServer
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  AdminService.BlockingInterface getAdmin(final ServerName serverName) throws IOException;

  /**
   * Establishes a connection to the region server at the specified address, and returns
   * a region client protocol.
   *
   * @param serverName
   * @return ClientProtocol proxy for RegionServer
   * @throws IOException if a remote or network exception occurs
   *
   */
  @Override
  ClientService.BlockingInterface getClient(final ServerName serverName) throws IOException;

  /**
   * Establishes a connection to the region server at the specified address, and returns an
   * non-blocking region client protocol.
   * @param serverName
   * @return Non-blocking proxy for RegionServer
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  ClientService.Interface getAsyncClient(final ServerName serverName) throws IOException;

  /**
   * Find region location hosting passed row
   * @param tableName table name
   * @param row Row to find.
   * @param reload If true do not use cache, otherwise bypass.
   * @return Location of row.
   * @throws IOException if a remote or network exception occurs
   */
  @Override
  HRegionLocation getRegionLocation(TableName tableName, byte [] row,
    boolean reload)
  throws IOException;

  /**
   * Clear any caches that pertain to server name <code>sn</code>.
   * @param sn A server name
   */
  @Override
  void clearCaches(final ServerName sn);

  /**
   * This function allows HBaseAdmin and potentially others to get a shared MasterService
   * connection.
   * @return The shared instance. Never returns null.
   * @throws MasterNotRunningException
   */
  @Override
  @Deprecated
  MasterKeepAliveConnection getKeepAliveMasterService()
  throws MasterNotRunningException;

  /**
   * @param serverName
   * @return true if the server is known as dead, false otherwise.
   * @deprecated internal method, do not use thru HConnection */
  @Override
  @Deprecated
  boolean isDeadServer(ServerName serverName);

  /**
   * @return Nonce generator for this HConnection; may be null if disabled in configuration.
   */
  @Override
  public NonceGenerator getNonceGenerator();

  /**
   * @return Default AsyncProcess associated with this connection.
   */
  AsyncProcess getAsyncProcess();

  /**
   * Returns a new RpcRetryingCallerFactory from the given {@link Configuration}.
   * This RpcRetryingCallerFactory lets the users create {@link RpcRetryingCaller}s which can be
   * intercepted with the configured {@link RetryingCallerInterceptor}
   * @param conf
   * @return RpcRetryingCallerFactory
   */
  RpcRetryingCallerFactory getNewRpcRetryingCallerFactory(Configuration conf);
  
  /**
   * 
   * @return true if this is a managed connection.
   */
  boolean isManaged();

  /**
   * @return the current statistics tracker associated with this connection
   */
  ServerStatisticTracker getStatisticsTracker();

  /**
   * @return the configured client backoff policy
   */
  ClientBackoffPolicy getBackoffPolicy();

  /**
   * @return true when this connection uses a {@link org.apache.hadoop.hbase.codec.Codec} and so
   *         supports cell blocks rather than Protobuffed Cells.
   */
  boolean supportsCellBlock();

  /**
   * @return true when this client supports non-blocking interface with {@link RpcCallback}
   */
  boolean supportsNonBlockingInterface();
}
