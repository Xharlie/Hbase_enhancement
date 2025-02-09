/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.io.hfile;

import java.util.Iterator;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;

import com.google.common.annotations.VisibleForTesting;


/**
 * CombinedBlockCache is an abstraction layer that combines
 * {@link LruBlockCache} and {@link BucketCache}. The smaller lruCache is used
 * to cache bloom blocks and index blocks.  The larger l2Cache is used to
 * cache data blocks. {@link #getBlock(BlockCacheKey, boolean, boolean, boolean)} reads
 * first from the smaller lruCache before looking for the block in the l2Cache.  Blocks evicted
 * from lruCache are put into the bucket cache. 
 * Metrics are the combined size and hits and misses of both caches.
 * 
 */
@InterfaceAudience.Private
public class CombinedBlockCache implements ResizableBlockCache, HeapSize {
  protected final LruBlockCache lruCache;
  protected final BlockCache l2Cache;
  protected final CombinedCacheStats combinedCacheStats;

  public CombinedBlockCache(LruBlockCache lruCache, BlockCache l2Cache) {
    this.lruCache = lruCache;
    this.l2Cache = l2Cache;
    this.combinedCacheStats = new CombinedCacheStats(lruCache.getStats(),
        l2Cache.getStats());
  }

  @Override
  public long heapSize() {
    long l2size = 0;
    if (l2Cache instanceof HeapSize) {
      l2size = ((HeapSize) l2Cache).heapSize();
    }
    return lruCache.heapSize() + l2size;
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf, boolean inMemory,
      final boolean cacheDataInL1) {
    boolean metaBlock = buf.getBlockType().getCategory() != BlockCategory.DATA;
    if (metaBlock || cacheDataInL1) {
      lruCache.cacheBlock(cacheKey, buf, inMemory, cacheDataInL1);
    } else {
      l2Cache.cacheBlock(cacheKey, buf, inMemory, false);
    }
  }

  @Override
  public void cacheBlock(BlockCacheKey cacheKey, Cacheable buf) {
    cacheBlock(cacheKey, buf, false, false);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching,
      boolean repeat, boolean updateCacheMetrics) {
    return getBlock(cacheKey, caching, repeat, updateCacheMetrics, null);
  }

  @Override
  public Cacheable getBlock(BlockCacheKey cacheKey, boolean caching,
      boolean repeat, boolean updateCacheMetrics, BlockType blockType) {
    // TODO: is there a hole here, or just awkwardness since in the lruCache getBlock
    // we end up calling l2Cache.getBlock.
    return lruCache.containsBlock(cacheKey)?
        lruCache.getBlock(cacheKey, caching, repeat, updateCacheMetrics):
        l2Cache.getBlock(cacheKey, caching, repeat, updateCacheMetrics);
  }

  @Override
  public boolean evictBlock(BlockCacheKey cacheKey) {
    return lruCache.evictBlock(cacheKey) || l2Cache.evictBlock(cacheKey);
  }

  @Override
  public int evictBlocksByHfileName(String hfileName) {
    return lruCache.evictBlocksByHfileName(hfileName)
        + l2Cache.evictBlocksByHfileName(hfileName);
  }

  @Override
  public CacheStats getStats() {
    return this.combinedCacheStats;
  }

  @Override
  public void shutdown() {
    lruCache.shutdown();
    l2Cache.shutdown();
  }

  @Override
  public long size() {
    return lruCache.size() + l2Cache.size();
  }

  @Override
  public long getFreeSize() {
    return lruCache.getFreeSize() + l2Cache.getFreeSize();
  }

  @Override
  public long getCurrentSize() {
    return lruCache.getCurrentSize() + l2Cache.getCurrentSize();
  }

  @Override
  public long getBlockCount() {
    return lruCache.getBlockCount() + l2Cache.getBlockCount();
  }

  private static class CombinedCacheStats extends CacheStats {
    private final CacheStats lruCacheStats;
    private final CacheStats bucketCacheStats;

    CombinedCacheStats(CacheStats lbcStats, CacheStats fcStats) {
      super("CombinedBlockCache");
      this.lruCacheStats = lbcStats;
      this.bucketCacheStats = fcStats;
    }

    @Override
    public long getRequestCount() {
      return lruCacheStats.getRequestCount()
          + bucketCacheStats.getRequestCount();
    }

    @Override
    public long getRequestCachingCount() {
      return lruCacheStats.getRequestCachingCount()
          + bucketCacheStats.getRequestCachingCount();
    }

    @Override
    public long getMissCount() {
      return lruCacheStats.getMissCount() + bucketCacheStats.getMissCount();
    }

    @Override
    public long getMissCachingCount() {
      return lruCacheStats.getMissCachingCount()
          + bucketCacheStats.getMissCachingCount();
    }

    @Override
    public long getHitCount() {
      return lruCacheStats.getHitCount() + bucketCacheStats.getHitCount();
    }

    @Override
    public long getHitCachingCount() {
      return lruCacheStats.getHitCachingCount()
          + bucketCacheStats.getHitCachingCount();
    }

    @Override
    public long getDataHitCachingCount() {
      return lruCacheStats.getDataHitCachingCount()
          + bucketCacheStats.getDataHitCachingCount();
    }

    @Override
    public long getDataRequestCachingCount() {
      return lruCacheStats.getDataRequestCachingCount()
          + bucketCacheStats.getDataRequestCachingCount();
    }

    @Override
    public double getDataHitCachingRatio() {
      return ((float)getDataHitCachingCount()/(float)getDataRequestCachingCount());
    }

    @Override
    public long getDataHitCount() {
      return lruCacheStats.getDataHitCount()
          + bucketCacheStats.getDataHitCount();
    }

    @Override
    public double getMetaHitRatio() {
      return ((float) lruCacheStats.getMetaHitCount() / (float) lruCacheStats.getMetaRequestCount());
    }

    @Override
    public long getDataRequestCount() {
      return lruCacheStats.getDataRequestCount()
          + bucketCacheStats.getDataRequestCount();
    }

    @Override
    public double getDataHitRatio() {
      return ((float) getDataHitCount() / (float) getDataRequestCount());
    }

    @Override
    public double getBucketCacheHitRatio() {
      return bucketCacheStats.getHitRatio();
    }
    @Override
    public long getMetaHitCachingCount() {
      return lruCacheStats.getMetaHitCachingCount()
          + bucketCacheStats.getMetaHitCachingCount();
    }

    @Override
    public long getMetaRequestCachingCount() {
      return lruCacheStats.getMetaRequestCachingCount()
          + bucketCacheStats.getMetaRequestCachingCount();
    }

    @Override
    public double getMetaHitCachingRatio() {
      return ((float)getMetaHitCachingCount()/(float)getMetaRequestCachingCount());
    }

    @Override
    public long getEvictionCount() {
      return lruCacheStats.getEvictionCount()
          + bucketCacheStats.getEvictionCount();
    }

    @Override
    public long getEvictedCount() {
      return lruCacheStats.getEvictedCount()
          + bucketCacheStats.getEvictedCount();
    }

    @Override
    public double getHitRatioPastNPeriods() {
      double ratio = ((double) (lruCacheStats.getSumHitCountsPastNPeriods() + bucketCacheStats
          .getSumHitCountsPastNPeriods()) / (double) (lruCacheStats
          .getSumRequestCountsPastNPeriods() + bucketCacheStats
          .getSumRequestCountsPastNPeriods()));
      return Double.isNaN(ratio) ? 0 : ratio;
    }

    @Override
    public double getHitCachingRatioPastNPeriods() {
      double ratio = ((double) (lruCacheStats
          .getSumHitCachingCountsPastNPeriods() + bucketCacheStats
          .getSumHitCachingCountsPastNPeriods()) / (double) (lruCacheStats
          .getSumRequestCachingCountsPastNPeriods() + bucketCacheStats
          .getSumRequestCachingCountsPastNPeriods()));
      return Double.isNaN(ratio) ? 0 : ratio;
    }
  }

  @Override
  public Iterator<CachedBlock> iterator() {
    return new BlockCachesIterator(getBlockCaches());
  }

  @Override
  public BlockCache[] getBlockCaches() {
    return new BlockCache [] {this.lruCache, this.l2Cache};
  }

  @Override
  public void setMaxSize(long size) {
    this.lruCache.setMaxSize(size);
  }

  @Override
  public void returnBlock(BlockCacheKey cacheKey, Cacheable block) {
    // A noop
    this.lruCache.returnBlock(cacheKey, block);
    this.l2Cache.returnBlock(cacheKey, block);
  }

  @VisibleForTesting
  public int getRefCount(BlockCacheKey cacheKey) {
    return ((BucketCache) this.l2Cache).getRefCount(cacheKey);
  }

  @Override
  public long getUsedSize() {
    return l2Cache.getUsedSize();
  }

  @Override
  public long getRealUsedSize() {
    return l2Cache.getRealUsedSize();
  }

  @Override
  public long getCapacity() {
    return l2Cache.getCapacity();
  }
}
