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

package org.apache.hadoop.hbase.tool;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Probe result returned to
 */
class ProbeResult {
  private byte[] regionName;
  private boolean result;
  private String reason;

  public ProbeResult(byte[] rn, boolean result, String reason) {
    this.regionName = rn;
    this.result = result;
    this.reason = reason;
  }

  public byte[] getRegionName() {
    return regionName;
  }

  public String getReason() {
    return reason;
  }

  boolean getResult() {
    return result;
  }
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("regions: ").append(HRegionInfo.encodeRegionName(regionName)).append(", ");
    sb.append("result: ").append(result).append(", ");
    sb.append("reason: ").append(reason).append("  ");
    return  sb.toString();
  }
}

abstract class ProbeTask implements Callable<ProbeResult> {

  @Override
  public ProbeResult call() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  protected Scan initializeScanTask(String startKey) {
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(startKey));
    scan.setBatch(1);
    scan.setCaching(1);
    scan.setMaxVersions(1);
    scan.setCacheBlocks(false);
    scan.setRaw(false);
    return scan;
  }
}

/**
 * do scan operation to a region and return if the scan is succeed
 */
class ScanTask extends ProbeTask {
  private Configuration conf;
  private byte[] regionName;
  Connection connection = null;

  public ScanTask(Configuration conf, byte[] regionName, Connection con) {
    this.conf = conf;
    this.regionName = regionName;
    this.connection = con;
  }

  @Override
  public ProbeResult call() throws Exception {
    ProbeResult scanResult = doScanOperation(conf, regionName);
    boolean succeed = scanResult.getResult();
    if (!succeed) {
      return scanResult;
    } else {
      return null;
    }
  }

  private ProbeResult doScanOperation(Configuration conf, byte[] regionName) {
    TableName tableName = HRegionInfo.getTable(regionName);
    String startKey = null;
    try {
      startKey = Bytes.toString(HRegionInfo.getStartKey(regionName));
    } catch (IOException e) {
      System.err.println("failed get startKey : " + e);
    }
    if (null == tableName || null == startKey) {
      return new ProbeResult(regionName, false, "tableName or startKey is null");
    }

    // get table instance
    Table table = null;
    try {
      table = connection.getTable(tableName);
    } catch (IOException e) {
      return new ProbeResult(regionName, false, "cant get htable");
    }
    // initialize scanner
    Scan scan = null;
    scan = initializeScanTask(startKey);
    // calculate latency
    long startTs = System.currentTimeMillis();
    long endTs = -1;
    ResultScanner scanner = null;
    try {
      /**
       * here we use scan instead of get to do probe work due to:
       * 1. we can adjust the data size geting from RS conveniently if needed.
       * 2. get one data from RS will need 2 rpc calls at least, which will more safe
       * and avoid kill RS incollectly.
       */
      scanner = table.getScanner(scan);
      Iterator<Result> iter = scanner.iterator();
      if (iter.hasNext()) {
        iter.next();
      }
      endTs = System.currentTimeMillis();
    } catch (Throwable e) {
      e.printStackTrace();
      System.err.println("hbase latency scan exception: [" + Bytes.toString(regionName) + "]  error : " + e.getMessage());
      return new ProbeResult(regionName, false, e.getMessage());
    } finally {
      if (null != scanner) {
        scanner.close();
      }
      if (null != table) {
        try {
          table.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }

    long scanTime = endTs - startTs;
    return new ProbeResult(regionName, true, "region[" + Bytes.toString(regionName) + "] scanTime:" + scanTime);
  }
}

/**
 * do write to a dummp table. not avalilable yet
 */
class PutTask extends ProbeTask {
  private Configuration conf;
  private byte[] regionName;

  public PutTask(Configuration conf, byte[] regionName) {
    this.conf = conf;
    this.regionName = regionName;
  }

  @Override
  public ProbeResult call() throws Exception {
    ProbeResult scanResult = doPutOperation(conf, regionName);
    boolean succeed = scanResult.getResult();
    if (!succeed) {
      return scanResult;
    } else {
      return null;
    }
  }

  private ProbeResult doPutOperation(Configuration conf, byte[] regionName) {
    return new ProbeResult(regionName, true, "");
  }
}
