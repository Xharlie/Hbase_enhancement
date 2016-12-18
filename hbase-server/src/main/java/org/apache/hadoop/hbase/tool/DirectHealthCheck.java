package org.apache.hadoop.hbase.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by xharlie on 8/12/16.
 */
public class DirectHealthCheck {
  private static Log LOG = LogFactory.getLog(DirectHealthChecker.class);
  private static final String DEFAULT_HQUEUE_COPROCESSOR_NAME = "com.etao.hadoop.hbase.queue.coprocessor.HQueueCoprocessor";
  private volatile HRegionServer rs;
  private double failedThreshold = 1.0;
  private int threadPoolSize = 0;
  private int sampledRegionCount = 0;
  private int onlineRegionCount = 0;
  private String localHostName;
  private long opTimeout = 0;

  private Configuration conf;
  private List<Region> onlineRegions;
  private ServerName localServerName;
  private Connection connection;

  public DirectHealthCheck(int threadPoolSize, float failedThreshold,
                           int sampledRegionCount, long opTimeout, String currentServerName, HRegionServer rs, Configuration config) throws IllegalArgumentException {
    parseArguments(threadPoolSize, failedThreshold, sampledRegionCount, opTimeout, currentServerName);
    this.rs = rs;
    this.conf = config;
    this.threadPoolSize = threadPoolSize;
    this.failedThreshold = failedThreshold;
    this.sampledRegionCount = sampledRegionCount;
    this.opTimeout = opTimeout;
    this.localHostName = currentServerName;
    this.localServerName = ServerName.valueOf(currentServerName);
  }

  public boolean execute(List<Region> onlineRegions) throws IOException {
    this.connection = ConnectionFactory.createConnection(this.conf);
    this.onlineRegions = onlineRegions;
    boolean succeed = false;
    try {
      succeed = probeLocalHost();
    } catch (Exception e) {
      LOG.info("Health Check catch exception......succeed == false");
      succeed = false;
    } finally {
      this.connection.close();
    }
    return succeed;
  }


  public Configuration getConf() {
    return this.conf;
  }

  public void setLocalHostName(String l) {
    localHostName = l;
  }

  public List<Region> getOnlineRegions() {
    return this.onlineRegions;
  }

  public int getSampledRegionCount() {
    return sampledRegionCount;
  }

  public void setSampledRegionCount(int t) {
    sampledRegionCount = t;
  }

  private boolean isRegionBelongToHqueue(HRegion region) throws IOException {
    HTableDescriptor tableDesc = region.getTableDesc();
    List<String> coprocessors = tableDesc.getCoprocessors();
    if (null == coprocessors || coprocessors.isEmpty()) {
      return false;
    }
    for (String cpClass : coprocessors) {
      if (cpClass.equalsIgnoreCase(DEFAULT_HQUEUE_COPROCESSOR_NAME)) {
        LOG.info(region.getRegionInfo().getEncodedName() + " belongs to table: "
                + tableDesc.getTableName() + " which is hqueue.");
        return true;
      }
    }
    return false;
  }

  /**
   * select regions to scan
   *
   * @param onlineRegions should not be null or empty.
   * @return true if we actually select some regions to scan false if regions on
   * the server is empty
   * @throws IOException
   */
  public List<HRegion> selectProbeRegions(List<Region> onlineRegions) throws IOException {
    List<HRegion> selectedRegions = new LinkedList<HRegion>();
    onlineRegionCount = onlineRegions.size();
    int loopCount = 0;

    if (onlineRegions.size() < sampledRegionCount) {
      //iterate all online regions, and remove hqueue regions
      for (Region region : onlineRegions) {
        if (!isRegionBelongToHqueue((HRegion) region)) {
          selectedRegions.add((HRegion) region);
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
          Region region = onlineRegions.get(temp);
          // exclude Hqueue region and empty region
          if (!isRegionBelongToHqueue((HRegion) region) && region.getStartKey().length != 0) {
            selectedRegions.add((HRegion) region);
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
      LOG.info("select " + selectedRegions.size() + "regions, too less, will discard this probe.");
      selectedRegions.clear();
    }
    LOG.info("total regions is " + onlineRegionCount + " and select "
            + selectedRegions.size() + " regions with loopCount: " + loopCount);
    LOG.info("selected regions: ");
    for (HRegion sr : selectedRegions) {
      LOG.info(sr.getRegionInfo().getRegionNameAsString() + "  ");
    }
    return selectedRegions;
  }

  private List<ProbeResult> reportProbResult(List<FutureTask<ProbeResult>> tasks,
                                             HashMap<FutureTask<ProbeResult>, HRegion> map) {
    List<ProbeResult> failedRegionList = new LinkedList<ProbeResult>();
    ProbeResult probeResult = null;
    // get all failed probe
    long startTime = System.currentTimeMillis();
    long deadlineTime = startTime + opTimeout;
    long timeout = 0;
    Date dateStart = new Date(startTime);
    Date dateDeadline = new Date(deadlineTime);
    Date dateCurrent = new Date();
    for (FutureTask<ProbeResult> task : tasks) {
      try {
        //this will waste long time if many regions get timeout ?
        long currentTime = System.currentTimeMillis();
        timeout = deadlineTime - currentTime;
        dateCurrent.setTime(currentTime);
        LOG.info("health Check individual check: start time:" + dateStart + "; deadline time:" + dateDeadline + "; current time:"
                + dateCurrent + "; target region: " + map.get(task).getRegionInfo().getRegionNameAsString());
        probeResult = task.get((timeout < 0) ? 0 : timeout, TimeUnit.MILLISECONDS);
        if (probeResult != null) {
          failedRegionList.add(probeResult);
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        HRegion region = map.get(task);
        failedRegionList.add(new ProbeResult(region.getRegionInfo().getRegionName(), false, "timeout; Started at:"
                + dateStart.toString() + "; takes more than " + opTimeout + " miliseconds"));
        Thread.currentThread().interrupt();
        continue;
      } catch (Exception e) {
        HRegion region = map.get(task);
        failedRegionList.add(new ProbeResult(region.getRegionInfo().getRegionName(), false, "Exception; Started at:"
                + dateStart.toString() + "; Trace: " + e.getStackTrace()));
        continue;
      }
    }

    return failedRegionList;
  }

  public boolean probeLocalHost() throws IOException {
    LOG.info("staring to probe the cluster");
    if (null == onlineRegions || onlineRegions.isEmpty()) {
      LOG.info("no online regions on " + localHostName + "  but will return true to avoid this rs killed.");
      return true;
    }

    List<HRegion> selectedRegions = selectProbeRegions(onlineRegions);
    if (selectedRegions.isEmpty()) {
      LOG.info("select none regions on " + localHostName + "  but still return true to avoid this rs killed.");
      this.rs.setDirectHealthCheckFailedRegionCount(0);
      this.rs.setDirectHealthCheckSelectedRegionCount(0);
      this.rs.setDirectHealthCheckFailedRatio(0);
      return true;
    }

    // get executor and do parallel scan
    ExecutorService probeExecutor = null;
    HashMap<FutureTask<ProbeResult>, HRegion> map = new HashMap<>();
    probeExecutor = Executors.newFixedThreadPool(threadPoolSize);
    List<FutureTask<ProbeResult>> tasks = new LinkedList<FutureTask<ProbeResult>>();
    for (HRegion region : selectedRegions) {
      DirectGetTask getTask = new DirectGetTask(conf, region, connection, localServerName);
      FutureTask<ProbeResult> task = new FutureTask<>(getTask);
      map.put(task, region);
      probeExecutor.submit(task);
      tasks.add(task);
    }
    List<ProbeResult> failedRegions = reportProbResult(tasks, map);
    double failedRatio = (selectedRegions.size() == 0) ? 0 : ((double) failedRegions.size()) / selectedRegions.size();
    boolean succeed = (failedRatio < failedThreshold);
    this.rs.setDirectHealthCheckFailedRegionCount(failedRegions.size());
    this.rs.setDirectHealthCheckSelectedRegionCount(selectedRegions.size());
    this.rs.setDirectHealthCheckFailedRatio(failedRatio);
    //only for test
        /*
        Thread.sleep(60000);
        if (new Random().nextInt(10) % 2 == 0) {
          System.out.println("will throw exception....");
          throw new IOException("exp");
        }
        */
    LOG.info("\nfailed threshold : " + failedThreshold);
    for (ProbeResult fr : failedRegions) {
      LOG.info("Health Check, get failed regions :" + fr.toString());
    }
    if(failedRegions.size()==0) LOG.info("Health Check, no failed regions.");
    probeExecutor.shutdown();
    return succeed;
  }

  private void parseArguments(int threadPoolSize, float failedThreshold,
                              int sampledRegionCount, long scanTimeout, String currentServerName) {

    if (threadPoolSize <= 0 || threadPoolSize > 30) {
      throw new IllegalArgumentException("Health check invalid thread pool size: " + threadPoolSize);
    } else {
      LOG.info("Health check thread pool size:" + threadPoolSize);
    }
    if (failedThreshold < 0 || failedThreshold > 1) {
      throw new IllegalArgumentException("Health check invalid failed threshold: " + failedThreshold);
    } else {
      LOG.info("Health check failed threshold:" + failedThreshold);
    }
    if (sampledRegionCount <= 0) {
      throw new IllegalArgumentException("Health check invalid sampled region count: " + sampledRegionCount);
    } else {
      LOG.info("Health check sampled region count:" + sampledRegionCount);
    }
    if (scanTimeout <= 0) {
      throw new IllegalArgumentException("Health check invalid scan timeout: " + scanTimeout);
    } else {
      LOG.info("Health check scan timeout:" + scanTimeout);
    }
    //should check the host name format ?
    if (currentServerName == null || currentServerName.equals("localhost")) {
      throw new IllegalArgumentException("Health check invalid local host name: " + currentServerName);
    } else {
      LOG.info("Health check local host name:" + currentServerName);
    }
  }


}
