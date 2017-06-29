package org.apache.hadoop.hbase.embedded;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Options {
  public static final byte[] DEFAULT_FAMILY = Bytes.toBytes("cf");
  private final Log LOG = LogFactory.getLog(Options.class);

  byte[] tableNameBytes;
  TableName tableName;
  byte[][] splitKeys;
  int blockSize;
  Algorithm compression;
  Durability durability;
  ArrayList<byte[]> families;

  public Options() {
    this.blockSize = HColumnDescriptor.DEFAULT_BLOCKSIZE;
    this.compression = Algorithm.valueOf(HColumnDescriptor.DEFAULT_COMPRESSION.toUpperCase());
    this.durability = Durability.USE_DEFAULT;
  }

  public byte[] getTableNameBytes() {
    return tableNameBytes;
  }

  public void setTableNameBytes(byte[] tableNameBytes) {
    this.tableNameBytes = tableNameBytes;
    this.tableName = TableName.valueOf(tableNameBytes);
  }

  public TableName getTableName() {
    return tableName;
  }

  public void setTableName(TableName tableName) {
    this.tableName = tableName;
    this.tableNameBytes = tableName.getName();
  }

  public byte[][] getSplitKeys() {
    return splitKeys;
  }

  public void setSplitKeys(byte[][] splitKeys) {
    this.splitKeys = splitKeys;
  }

  public int getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(int blockSize) {
    this.blockSize = blockSize;
  }

  public Algorithm getCompression() {
    return compression;
  }

  public void setCompression(String compressionString) {
    Algorithm compression;
    try {
      compression = Algorithm.valueOf(compressionString.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOG.debug("Unknown compression [" + compressionString
              + "], supported compression algorithm including " + Arrays.asList(Algorithm.values())
              + ", please check your input",
          e);
      throw e;
    }
    this.compression = compression;
  }

  public Durability getDurability() {
    return durability;
  }

  public void setDurability(String durabilityString) {
    Durability durability;
    try {
      durability = Durability.valueOf(durabilityString.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOG.debug("Unknown durability [" + durabilityString + "], supported durability including "
              + Arrays.asList(Durability.values()) + ", please check your input",
          e);
      throw e;
    }
    this.durability = durability;
  }

  public void addColumnFamily(String familyName) {
    if (families == null) {
      families = new ArrayList<byte[]>();
    }
    families.add(Bytes.toBytes(familyName));
  }

  public ArrayList<byte[]> getFamilies() {
    return families;
  }

  public static void main(String[] args) {
    Options opts = new Options();
    opts.setDurability("async_wal");
    opts.setCompression("none");
  }

}
