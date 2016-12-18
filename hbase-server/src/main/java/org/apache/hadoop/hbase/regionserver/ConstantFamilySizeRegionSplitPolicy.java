package org.apache.hadoop.hbase.regionserver;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class ConstantFamilySizeRegionSplitPolicy extends RegionSplitPolicy {
  public static final Log LOG = LogFactory.getLog(ConstantFamilySizeRegionSplitPolicy.class);
  private Map<String, Long> familyMaxFileSizes;

  @Override
  protected void configureForRegion(HRegion region) {
    super.configureForRegion(region);
    familyMaxFileSizes = new TreeMap<String, Long>();
    HTableDescriptor htd = region.getTableDesc();
    long maxFileSize = htd.getMaxFileSize();
    if (maxFileSize == HConstants.DEFAULT_MAX_FILE_SIZE) {
      maxFileSize =
          getConf().getLong("hbase.hregion.max.filesize", HConstants.DEFAULT_MAX_FILE_SIZE);
    }

    HColumnDescriptor[] hcds = region.getTableDesc().getColumnFamilies();
    for (HColumnDescriptor hcd : hcds) {
      long maxFamilyFileSize = maxFileSize;
      String family = hcd.getNameAsString();
      String maxFamilyFileSizeStr = hcd.getValue(HTableDescriptor.MAX_FILESIZE);
      if (null != maxFamilyFileSizeStr) {
        maxFamilyFileSize = Long.parseLong(maxFamilyFileSizeStr);
      }
      familyMaxFileSizes.put(family, maxFamilyFileSize);
    }
  }

  @Override
  protected boolean shouldSplit() {
    boolean force = region.shouldForceSplit();
    boolean foundABigStore = false;
    for (Store store : region.getStores()) {
      if ((!store.canSplit())) {
        return false;
      }

      if (store.getSize() > familyMaxFileSizes.get(store.getFamily().getNameAsString())) {
        foundABigStore = true;
        break;
      }
    }

    return foundABigStore || force;
  }

  @Override
  protected byte[] getSplitPoint() {
    // use explicit split point first
    byte[] explicitSplitPoint = this.region.getExplicitSplitPoint();
    if (explicitSplitPoint != null) {
      return explicitSplitPoint;
    }

    // get split point by store size ratio
    List<Store> stores = region.getStores();
    byte[] splitPointFromLargestStoreRatio = null;
    String splitFamilyName = null;
    long splitStoreSize = 0;
    float largestStoreRatio = 0;
    for (Store store : stores) {
      long storeSize = store.getSize();
      long maxFileSize = familyMaxFileSizes.get(store.getFamily().getNameAsString());
      float storeRatio = (float) storeSize / (float) maxFileSize;
      byte[] splitPoint = store.getSplitPoint();
      if (splitPoint != null && largestStoreRatio < storeRatio) {
        splitPointFromLargestStoreRatio = splitPoint;
        largestStoreRatio = storeRatio;
        splitFamilyName = store.getFamily().getNameAsString();
        splitStoreSize = storeSize;
      }
    }

    if (null != splitFamilyName) {
      LOG.info("split region: " + region.getRegionInfo().getRegionNameAsString()
          + " , split family: " + splitFamilyName + ", family size: " + splitStoreSize
          + " , split point: " + Bytes.toString(splitPointFromLargestStoreRatio));
    }

    return splitPointFromLargestStoreRatio;
  }
}
