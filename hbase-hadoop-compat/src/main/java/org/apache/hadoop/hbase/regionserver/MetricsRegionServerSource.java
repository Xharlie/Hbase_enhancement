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

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.metrics.JvmPauseMonitorSource;

/**
 * Interface for classes that expose metrics about the regionserver.
 */
public interface MetricsRegionServerSource extends BaseSource, JvmPauseMonitorSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Server";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase RegionServer";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  /**
   * Update the Put time histogram
   *
   * @param t time it took
   */
  void updatePut(long t);

  /**
   * Update the Delete time histogram
   *
   * @param t time it took
   */
  void updateDelete(long t);

  /**
   * Update the Get time histogram .
   *
   * @param t time it took
   */
  void updateGet(long t);

  /**
   * Update the Increment time histogram.
   *
   * @param t time it took
   */
  void updateIncrement(long t);

  /**
   * Update the Append time histogram.
   *
   * @param t time it took
   */
  void updateAppend(long t);

  /**
   * Update the Replay time histogram.
   *
   * @param t time it took
   */
  void updateReplay(long t);

  /**
   * Update the scan size.
   *
   * @param scanSize size of the scan
   */
  void updateScanSize(long scanSize);

  /**
   * Update the scan time.
   * */
  void updateScanTime(long t);

  /**
   * Increment the number of slow Puts that have happened.
   */
  void incrSlowPut();

  /**
   * Increment the number of slow Deletes that have happened.
   */
  void incrSlowDelete();

  /**
   * Increment the number of slow Gets that have happened.
   */
  void incrSlowGet();

  /**
   * Increment the number of slow Increments that have happened.
   */
  void incrSlowIncrement();

  /**
   * Increment the number of slow Appends that have happened.
   */
  void incrSlowAppend();

  /**
   * Update the split transaction time histogram
   * @param t time it took, in milliseconds
   */
  void updateSplitTime(long t);

  /**
   * Increment number of a requested splits
   */
  void incrSplitRequest();

  /**
   * Increment number of successful splits
   */
  void incrSplitSuccess();

  /**
   * Update the flush time histogram
   * @param t time it took, in milliseconds
   */
  void updateFlushTime(long t);

  // Strings used for exporting to metrics system.
  String REGION_COUNT = "regionCount";
  String REGION_COUNT_DESC = "Number of regions";
  String STORE_COUNT = "storeCount";
  String STORE_COUNT_DESC = "Number of Stores";
  String WALFILE_COUNT = "hlogFileCount";
  String WALFILE_COUNT_DESC = "Number of WAL Files";
  String WALFILE_SIZE = "hlogFileSize";
  String WALFILE_SIZE_DESC = "Size of all WAL Files";
  String STOREFILE_COUNT = "storeFileCount";
  String STOREFILE_COUNT_DESC = "Number of Store Files";
  String MEMSTORE_SIZE = "memStoreSize";
  String MEMSTORE_SIZE_DESC = "Size of the memstore";
  String STOREFILE_SIZE = "storeFileSize";
  String STOREFILE_SIZE_DESC = "Size of storefiles being served.";
  String TOTAL_REQUEST_COUNT = "totalRequestCount";
  String TOTAL_REQUEST_COUNT_DESC =
      "Total number of requests this RegionServer has answered.";
  String READ_REQUEST_COUNT = "readRequestCount";
  String READ_REQUEST_COUNT_DESC =
      "Number of read requests this region server has answered.";
  String WRITE_REQUEST_COUNT = "writeRequestCount";
  String WRITE_REQUEST_COUNT_DESC =
      "Number of mutation requests this region server has answered.";
  String PUT_REQUEST_COUNT = "putRequestCount";
  String PUT_REQUEST_COUNT_DESC =
      "Number of put requests this region server has answered.";
  String MULTI_GET_REQUEST_COUNT = "multiGetRequestCount";
  String MULTI_GET_REQUEST_COUNT_DESC =
      "Number of get requests through multi call this region server has answered.";
  String SCAN_CACHING_REQUEST_COUNT = "scanCachingRequestCount";
  String SCAN_CACHING_REQUEST_COUNT_DESC =
      "Number of scan requests (counting caching in) this region server has answered.";
  String RPC_GET_REQUEST_COUNT = "rpcGetRequestCount";
  String RPC_GET_REQUEST_COUNT_DESC =
      "Number of rpc get requests this region server has answered.";
  String RPC_SCAN_REQUEST_COUNT = "rpcScanRequestCount";
  String RPC_SCAN_REQUEST_COUNT_DESC =
      "Number of rpc scan requests this region server has answered.";
  String RPC_MULTI_REQUEST_COUNT = "rpcMultiRequestCount";
  String RPC_MULTI_REQUEST_COUNT_DESC =
      "Number of rpc multi requests this region server has answered.";
  String RPC_MUTATE_REQUEST_COUNT = "rpcMutateRequestCount";
  String RPC_MUTATE_REQUEST_COUNT_DESC =
      "Number of rpc mutation requests this region server has answered.";
  String ACTIVE_RPC_HANDLER_COUNT = "activeRpcHandlerCount";
  String ACTIVE_RPC_HANDLER_COUNT_DESC = "Number of active rpc handlers on this region server.";
  String RPC_TOTAL_SLOW_CALLS = "totalSlowCalls";
  String RPC_TOTAL_SLOW_CALLS_DESC = "Number of slow rpc calls this region server has answered.";
  String RPC_TOTAL_CALLS = "totalCalls";
  String RPC_TOTAL_CALLS_DESC = "Number of rpc calls this region server has answered.";
  String RPC_INC_SLOW_CALLS = "incrementalSlowCalls";
  String RPC_INC_SLOW_CALLS_DESC =
      "Incremental number of slow rpc calls this region server has answered.";
  String RPC_INC_CALLS = "incrementalCalls";
  String RPC_INC_CALLS_DESC = "Incremental number of rpc calls this region server has answered.";
  String CHECK_MUTATE_FAILED_COUNT = "checkMutateFailedCount";
  String CHECK_MUTATE_FAILED_COUNT_DESC =
      "Number of Check and Mutate calls that failed the checks.";
  String CHECK_MUTATE_PASSED_COUNT = "checkMutatePassedCount";
  String CHECK_MUTATE_PASSED_COUNT_DESC =
      "Number of Check and Mutate calls that passed the checks.";
  String STOREFILE_INDEX_SIZE = "storeFileIndexSize";
  String STOREFILE_INDEX_SIZE_DESC = "Size of indexes in storefiles on disk.";
  String STATIC_INDEX_SIZE = "staticIndexSize";
  String STATIC_INDEX_SIZE_DESC = "Uncompressed size of the static indexes.";
  String STATIC_BLOOM_SIZE = "staticBloomSize";
  String STATIC_BLOOM_SIZE_DESC =
      "Uncompressed size of the static bloom filters.";
  String NUMBER_OF_MUTATIONS_WITHOUT_WAL = "mutationsWithoutWALCount";
  String NUMBER_OF_MUTATIONS_WITHOUT_WAL_DESC =
      "Number of mutations that have been sent by clients with the write ahead logging turned off.";
  String DATA_SIZE_WITHOUT_WAL = "mutationsWithoutWALSize";
  String DATA_SIZE_WITHOUT_WAL_DESC =
      "Size of data that has been sent by clients with the write ahead logging turned off.";
  String PERCENT_FILES_LOCAL = "percentFilesLocal";
  String PERCENT_FILES_LOCAL_DESC =
      "The percent of HFiles that are stored on the local hdfs data node.";
  String PERCENT_FILES_LOCAL_SECONDARY_REGIONS = "percentFilesLocalSecondaryRegions";
  String PERCENT_FILES_LOCAL_SECONDARY_REGIONS_DESC =
    "The percent of HFiles used by secondary regions that are stored on the local hdfs data node.";
  String SPLIT_QUEUE_LENGTH = "splitQueueLength";
  String SPLIT_QUEUE_LENGTH_DESC = "Length of the queue for splits.";
  String COMPACTION_QUEUE_LENGTH = "compactionQueueLength";
  String LARGE_COMPACTION_QUEUE_LENGTH = "largeCompactionQueueLength";
  String SMALL_COMPACTION_QUEUE_LENGTH = "smallCompactionQueueLength";
  String COMPACTION_QUEUE_LENGTH_DESC = "Length of the queue for compactions.";
  String FLUSH_QUEUE_LENGTH = "flushQueueLength";
  String FLUSH_QUEUE_LENGTH_DESC = "Length of the queue for region flushes";
  String BLOCK_CACHE_FREE_SIZE = "blockCacheFreeSize";
  String BLOCK_CACHE_FREE_DESC =
      "Size of the block cache that is not occupied.";
  String BLOCK_CACHE_COUNT = "blockCacheCount";
  String BLOCK_CACHE_COUNT_DESC = "Number of block in the block cache.";
  String BLOCK_CACHE_SIZE = "blockCacheSize";
  String BLOCK_CACHE_SIZE_DESC = "Size of the block cache.";
  String BLOCK_CACHE_HIT_COUNT = "blockCacheHitCount";
  String BLOCK_CACHE_HIT_COUNT_DESC = "Count of the hit on the block cache.";
  String BLOCK_CACHE_MISS_COUNT = "blockCacheMissCount";
  String BLOCK_COUNT_MISS_COUNT_DESC =
      "Number of requests for a block that missed the block cache.";
  String BLOCK_CACHE_EVICTION_COUNT = "blockCacheEvictionCount";
  String BLOCK_CACHE_EVICTION_COUNT_DESC =
      "Count of the number of blocks evicted from the block cache.";
  String BLOCK_CACHE_HIT_PERCENT = "blockCacheCountHitPercent";
  String BLOCK_CACHE_HIT_PERCENT_DESC =
      "Percent of block cache requests that are hits";
  String BLOCK_CACHE_META_HIT_PERCENT = "blockCacheMetaHitPercent";
  String BLOCK_CACHE_META_HIT_PERCENT_DESC =
      "Percent of block cache requests on meta that are hits";
  String BLOCK_CACHE_USED_PERCENT = "blockCacheUsedPercent";
  String BLOCK_CACHE_USED_PERCENT_DESC =
      "Percent of block cache has been allocated for data";
  String BLOCK_CACHE_REAL_USED_PERCENT = "blockCacheRealUsedPercent";
  String BLOCK_CACHE_REAL_USED_PERCENT_DESC =
      "Percent of block cache has been allocated for data, excluding space padding";
  String BLOCK_CACHE_DATA_HIT_PERCENT = "blockCacheDataHitPercent";
  String BLOCK_CACHE_DATA_HIT_PERCENT_DESC =
      "Percent of requests on data that are hits";
  String BUCKET_CACHE_HIT_PERCENT = "bucketCacheHitPercent";
  String BUCKET_CACHE_HIT_PERCENT_DESC =
      "Percent of requests on bucket cache that are hits";
  String BLOCK_CACHE_EXPRESS_HIT_PERCENT = "blockCacheExpressHitPercent";
  String BLOCK_CACHE_EXPRESS_HIT_PERCENT_DESC =
      "The percent of the time that requests with the cache turned on hit the cache.";
  String BLOCK_CACHE_EXPRESS_META_HIT_PERCENT = "blockCacheExpressMetaHitPercent";
  String BLOCK_CACHE_EXPRESS_META_HIT_PERCENT_DESC =
      "The percent of the time that requests with the cache turned on hit the meta cache.";
  String BLOCK_CACHE_EXPRESS_DATA_HIT_PERCENT = "blockCacheExpressDataHitPercent";
  String BLOCK_CACHE_EXPRESS_DATA_HIT_PERCENT_DESC =
      "The percent of the time that requests with the cache turned on hit the data cache.";
  String BLOCK_CACHE_FAILED_INSERTION_COUNT = "blockCacheFailedInsertionCount";
  String BLOCK_CACHE_FAILED_INSERTION_COUNT_DESC = "Number of times that a block cache "
      + "insertion failed. Usually due to size restrictions.";
  String RS_START_TIME_NAME = "regionServerStartTime";
  String ZOOKEEPER_QUORUM_NAME = "zookeeperQuorum";
  String SERVER_NAME_NAME = "serverName";
  String CLUSTER_ID_NAME = "clusterId";
  String RS_START_TIME_DESC = "RegionServer Start Time";
  String ZOOKEEPER_QUORUM_DESC = "Zookeeper Quorum";
  String SERVER_NAME_DESC = "Server Name";
  String CLUSTER_ID_DESC = "Cluster Id";
  String UPDATES_BLOCKED_TIME = "updatesBlockedTime";
  String UPDATES_BLOCKED_DESC =
      "Number of MS updates have been blocked so that the memstore can be flushed.";
  String DELETE_KEY = "delete";
  String GET_KEY = "get";
  String INCREMENT_KEY = "increment";
  String MUTATE_KEY = "mutate";
  String APPEND_KEY = "append";
  String REPLAY_KEY = "replay";
  String SCAN_SIZE_KEY = "scanSize";
  String SCAN_TIME_KEY = "scanTime";

  String SLOW_MUTATE_KEY = "slowPutCount";
  String SLOW_GET_KEY = "slowGetCount";
  String SLOW_DELETE_KEY = "slowDeleteCount";
  String SLOW_INCREMENT_KEY = "slowIncrementCount";
  String SLOW_APPEND_KEY = "slowAppendCount";
  String SLOW_MUTATE_DESC =
      "The number of Multis that took over 1000ms to complete";
  String SLOW_DELETE_DESC =
      "The number of Deletes that took over 1000ms to complete";
  String SLOW_GET_DESC = "The number of Gets that took over 1000ms to complete";
  String SLOW_INCREMENT_DESC =
      "The number of Increments that took over 1000ms to complete";
  String SLOW_APPEND_DESC =
      "The number of Appends that took over 1000ms to complete";

  String FLUSHED_CELLS = "flushedCellsCount";
  String FLUSHED_CELLS_DESC = "The number of cells flushed to disk";
  String FLUSHED_CELLS_SIZE = "flushedCellsSize";
  String FLUSHED_CELLS_SIZE_DESC = "The total amount of data flushed to disk, in bytes";
  String COMPACTED_CELLS = "compactedCellsCount";
  String COMPACTED_CELLS_DESC = "The number of cells processed during minor compactions";
  String COMPACTED_CELLS_SIZE = "compactedCellsSize";
  String COMPACTED_CELLS_SIZE_DESC =
      "The total amount of data processed during minor compactions, in bytes";
  String MAJOR_COMPACTED_CELLS = "majorCompactedCellsCount";
  String MAJOR_COMPACTED_CELLS_DESC =
      "The number of cells processed during major compactions";
  String MAJOR_COMPACTED_CELLS_SIZE = "majorCompactedCellsSize";
  String MAJOR_COMPACTED_CELLS_SIZE_DESC =
      "The total amount of data processed during major compactions, in bytes";

  String BLOCKED_REQUESTS_COUNT = "blockedRequestCount";
  String BLOCKED_REQUESTS_COUNT_DESC = "The number of blocked requests because of memstore size is "
      + "larger than blockingMemStoreSize";

  String SPLIT_KEY = "splitTime";
  String SPLIT_REQUEST_KEY = "splitRequestCount";
  String SPLIT_REQUEST_DESC = "Number of splits requested";
  String SPLIT_SUCCESS_KEY = "splitSuccessCount";
  String SPLIT_SUCCESS_DESC = "Number of successfully executed splits";
  String FLUSH_KEY = "flushTime";

  String DIRECT_HEALTH_CHECK_SELECTED_REGION_COUNT = "directHealthCheckSelectedRegionCount";
  String DIRECT_HEALTH_CHECK_SELECTED_REGION_COUNT_DESC = "The number of regions selected for latest direct health check,";

  String DIRECT_HEALTH_CHECK_FAILED_REGION_COUNT = "directHealthCheckFailedRegionCount";
  String DIRECT_HEALTH_CHECK_FAILED_REGION_COUNT_DESC = "The number of failed probes for latest direct health check,"
          + " each probe would target a different online region randomly selected from all online regions";

  String DIRECT_HEALTH_CHECK_FAILED_RATIO = "directHealthCheckFailedRatio";
  String DIRECT_HEALTH_CHECK_FAILED_RARIO_DESC = "The failed ratio of direct health check, calculated " +
          "by dividing directHealthCheckFailedRegionCount by directHealthCheckSelectedRegionCount, ranged from 0 to 1 inclusive";

  String DIRECT_HEALTH_CHECK_NUM_UNHEALTHY = "directHealthCheckNumUnhealthy";
  String DIRECT_HEALTH_CHECK_NUM_UNHEALTHY_DESC = "the accumulative number of Times Unhealthy within the window, " +
          "which is the indicator of whether the RS should be stopped";
}
