package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.RegionGroupingProvider.RegionGroupingStrategy;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xharlie on 5/8/17.
 */
public class RoundRobinStageDistributionStrategy {
  protected static final Log LOG = LogFactory.getLog(RoundRobinStageDistributionStrategy.class);
  private int NUM_REGION_GROUPS;
  private int counter = 0;
  public ConcurrentHashMap<String, Integer> regionIndexPairs =
          new ConcurrentHashMap<String, Integer>();

  public int getIndex(String identifier) {
    Integer index = regionIndexPairs.get(identifier);
    if (null == index) {
      synchronized (regionIndexPairs) {
        index = regionIndexPairs.get(identifier);
        if (null == index) {
          index = counter++;
          regionIndexPairs.putIfAbsent(identifier, index);
        }
      }
    }
    return index % NUM_REGION_GROUPS;
  }

  public RoundRobinStageDistributionStrategy (int handlerCounts) {
    NUM_REGION_GROUPS = handlerCounts;
  }
}
