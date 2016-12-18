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
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HTableDescriptor;

public class HealthCheck {

  private static final String DEFAULT_HQUEUE_COPROCESSOR_NAME = "com.etao.hadoop.hbase.queue.coprocessor.HQueueCoprocessor";
  private double failedThreshold = 1.0;
  private int threadPoolSize = 0;
  private int sampledRegionCount = 0;
  private int onlineRegionCount = 0;
  private String localHostName = null;
  private long scanTimeout = 0;

  private Configuration conf = null;
  private List<byte[]> onlineRegions = null;
  private ServerName localServerName = null;
  private Connection connection = null;

  public HealthCheck(Configuration conf) throws IOException {
    this.conf = conf;
    this.connection = ConnectionFactory.createConnection(this.conf);
    onlineRegions = new LinkedList<byte[]>();
  }

  public Configuration getConf() {
    return this.conf;
  }

  public void setLocalHostName(String l) {
    localHostName = l;
  }
  public List<byte[]> getOnlineRegions() {
    return this.onlineRegions;
  }

  public int getSampledRegionCount() {
    return sampledRegionCount;
  }
  public void setSampledRegionCount(int t) {
    sampledRegionCount = t;
  }
  private  boolean isRegionBelongToHqueue(byte[] region) throws IOException {
    TableName tableName = HRegionInfo.getTable(region);
    Table table = null;
    if (null == tableName) {
      return false;
    }else {
      table = connection.getTable(tableName);
      if (null == table) {
        return false;
      }
    }

    HTableDescriptor tableDesc = table.getTableDescriptor();
    List<String> coprocessors = tableDesc.getCoprocessors();
    if (null == coprocessors || coprocessors.isEmpty()) {
        return false;
    }
    for (String cpClass : coprocessors) {
        if (cpClass.equalsIgnoreCase(DEFAULT_HQUEUE_COPROCESSOR_NAME)) {
          System.out.println(HRegionInfo.encodeRegionName(region) + " belongs to table: "
             + HRegionInfo.getTable(region) + " which is hqueue.");

            return true;
        }
    }
    return false;
  }

  private void initializeClusterInfo() throws IOException {
    HBaseAdmin admin = null;
    ClusterStatus cs = null;
    try {
      admin = (HBaseAdmin) connection.getAdmin();
      cs = admin.getClusterStatus();
      Set<byte[]> regions = cs.getLoad(localServerName).getRegionsLoad().keySet();

      for (byte[] region : regions) {
        onlineRegions.add(region);
      }

    } catch (Exception e) {
      System.out.println(e.getMessage());
      throw e;
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  /**
   * do necessary initialize for the cluster
   * @throws Exception
   */
  public void initialize(String[] args) throws Exception {
    try {
      parseArguments(args);
      initializeClusterInfo();
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * select regions to scan
   * @param onlineRegions should not be null or empty.
   * @return true if we actually select some regions to scan false if regions on
   *         the server is empty
   * @throws IOException
   */
  public List<byte[]> selectProbeRegions(List<byte[]> onlineRegions) throws IOException {
    List<byte[]> selectedRegions = new LinkedList<byte[]>();
    onlineRegionCount = onlineRegions.size();
    int loopCount = 0;

    if (onlineRegions.size() < sampledRegionCount) {
      //iterate all online regions, and remove hqueue regions
      for (byte[] region : onlineRegions) {
        if (!isRegionBelongToHqueue(region)) {
          selectedRegions.add(region);
        }
      }
    } else {
      // generate region server randomly
      Set<Integer> regionIndex = new HashSet<Integer>();
      Random random = new Random();
      while (selectedRegions.size() < sampledRegionCount) {
        int temp = Math.abs(random.nextInt() % onlineRegionCount);
        Integer t = new Integer(temp);
        if (!regionIndex.contains(t)) {
          regionIndex.add(t);
          byte[] region = onlineRegions.get(temp);
          if (!isRegionBelongToHqueue(region)) {
            selectedRegions.add(region);
          }
        }
        //dont retry too many times
        if (++loopCount >= 50 * sampledRegionCount) {
          break;
        }
      }
    }
    //if selected regions number is too small, just return empty list and discard this probe
    if (selectedRegions.size() < sampledRegionCount / 2 || selectedRegions.size() < 2) {
      System.out.println("select " + selectedRegions.size() + "regions, too less, will discard this probe.");
      selectedRegions.clear();
    }
    System.out.println("total regions is " + onlineRegionCount + " and select "
      + selectedRegions.size() + " regions with loopCount: " + loopCount);
    System.out.println("selected regions: ");
    for (byte[] sr : selectedRegions) {
      System.out.print(HRegionInfo.encodeRegionName(sr) + "  ");
    }
    return selectedRegions;
  }

  private List<ProbeResult> reportProbResult(List<FutureTask<ProbeResult>> tasks,
      HashMap<FutureTask<ProbeResult>, byte[]> map) {
    List<ProbeResult> failedRegionList = new LinkedList<ProbeResult>();
    ProbeResult probeResult = null;
    // get all failed probe
    for (FutureTask<ProbeResult> task : tasks) {
      try {
        //this will waste long time if many regions scan timeout ?
        probeResult = task.get(scanTimeout, TimeUnit.MILLISECONDS);
        if (probeResult != null) {
          failedRegionList.add(probeResult);
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        e.printStackTrace();
        byte[] region = map.get(task);
        failedRegionList.add(new ProbeResult(region, false, "timeout"));

        Thread.currentThread().interrupt();

        continue;
      }catch (Exception e) {
        e.printStackTrace();
        byte[] region = map.get(task);
        failedRegionList.add(new ProbeResult(region, false, "Exception"));
        continue;
      }
    }

    return failedRegionList;
  }

  public boolean probeLocalHost() throws IOException {
    System.out.println("staring to probe the cluster");
    if (null == onlineRegions || onlineRegions.isEmpty()) {
      System.out.println("no online regions on " + localHostName + "  but will return true to avoid this rs killed.");
      return true;
    }

    List<byte[]> selectedRegions = selectProbeRegions(onlineRegions);
    if (selectedRegions.isEmpty()) {
      System.out.println("select none regions on " + localHostName + "  but still return true to avoid this rs killed.");
      return true;
    }

    // get executor and do parallel scan
    ExecutorService scanExecutor = null;
    HashMap<FutureTask<ProbeResult>, byte[]> map = new HashMap<>();
    scanExecutor = Executors.newFixedThreadPool(threadPoolSize);
    List<FutureTask<ProbeResult>> tasks = new LinkedList<FutureTask<ProbeResult>>();
    for (byte[] region : selectedRegions) {
      ScanTask scanTask = new ScanTask(this.conf, region, this.connection);
      FutureTask<ProbeResult> task = new FutureTask<>(scanTask);
      map.put(task, region);
      scanExecutor.submit(task);
      tasks.add(task);
    }
    List<ProbeResult> failedRegions = reportProbResult(tasks, map);
    boolean succeed = ((((double) failedRegions.size()) / selectedRegions.size()) < failedThreshold);
    //only for test
    /*
    Thread.sleep(60000);
    if (new Random().nextInt(10) % 2 == 0) {
      System.out.println("will throw exception....");
      throw new IOException("exp");
    }
    */
    System.out.println("\nfailed threshold : " + failedThreshold);
    System.out.println("scan failed regions :");
    for (ProbeResult fr : failedRegions) {
      System.out.println(fr.toString());
    }

    scanExecutor.shutdown();

    return succeed;
  }

  private void parseArguments(String[] args) {
    if (args.length != 5) {
      System.err.println("unvalid argument number! + length : " + args.length);
      throw new IllegalArgumentException("unvalid argument number : " + args.length);
    }

    threadPoolSize = Integer.parseInt(args[0]);
    if (threadPoolSize <= 0 || threadPoolSize > 30) {
      throw new IllegalArgumentException("invalid thread pool size: " + threadPoolSize);
    }else {
      System.out.println("thread pool size:" + threadPoolSize);
    }
    failedThreshold = Float.parseFloat(args[1]);
    if (failedThreshold < 0 || failedThreshold > 1) {
      throw new IllegalArgumentException("invalid failed threshold: " + failedThreshold);
    }else {
      System.out.println("failed threshold:" + failedThreshold);
    }
    sampledRegionCount = Integer.parseInt(args[2]);
    if (sampledRegionCount <= 0) {
      throw new IllegalArgumentException("invalid sampled region count: " + sampledRegionCount);
    }else {
      System.out.println("sampled region count:" + sampledRegionCount);
    }
    scanTimeout = Long.parseLong(args[3]);
    if (scanTimeout <= 0) {
      throw new IllegalArgumentException("invalid scan timeout: " + scanTimeout);
    }else {
      System.out.println("scan timeout:" + scanTimeout);
    }

    localHostName = args[4];
    //should check the host name format ?
    if (localHostName == null || localHostName.equals("localhost")) {
      throw new IllegalArgumentException("invalid local host name: " + localHostName);
    }else {
      System.out.println("local host name:" + localHostName);
    }

    localServerName = ServerName.valueOf(localHostName);
  }

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    HealthCheck healthCheck = null;
    boolean succeed = false;
    try {
      healthCheck = new HealthCheck(conf);
      healthCheck.initialize(args);

      succeed = healthCheck.probeLocalHost();
      //close the connection
      healthCheck.connection.close();

    } catch (Exception e) {
      System.out.println("catch exception......succeed == false");
      succeed = false;
    } finally {
      System.out.println(succeed ? "probe succeed" : "\nprobe failed");
      // return value to shell to indicate if probe succeed or not
      System.exit(succeed ? 0 : 1);
    }
  }
}
