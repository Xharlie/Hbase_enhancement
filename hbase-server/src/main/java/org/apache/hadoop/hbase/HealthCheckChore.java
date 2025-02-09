/*
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
package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HealthChecker.HealthCheckerExitStatus;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Date;

/**
 * The Class HealthCheckChore for running health checker regularly.
 */
public class HealthCheckChore extends ScheduledChore {
  private static Log LOG = LogFactory.getLog(HealthCheckChore.class);
  private HealthChecker healthChecker;
  private Configuration config;
  private int threshold;
  // counter to record unhealthy count in a window
  private int numTimesUnhealthy = 0;
  // counter to record accumulative unhealthy count of the RS
  private int directHealthCheckNumUnhealthy = 0;
  private long failureWindow;
  private long startWindow;
  private HRegionServer rs;

  public HealthCheckChore(int sleepTime, Stoppable stopper, Configuration conf) {
    super("HealthChecker", stopper, sleepTime);
    LOG.info("Health Check Chore runs every " + StringUtils.formatTime(sleepTime));
    this.config = conf;
    int threadPoolSize = this.config.getInt(HConstants.HEALTH_THREAD_POOL_SIZE, HConstants.DEFAULT_HEALTH_THREAD_POOL_SIZE);
    float failedThreshold = this.config.getFloat(HConstants.HEALTH_FAILED_THRESHOLD, HConstants.DEFAULT_HEALTH_FAILED_THRESHOLD);
    int sampledRegionCount = this.config.getInt(HConstants.HEALTH_SAMPLED_REGION_COUNT, HConstants.DEFAULT_HEALTH_SAMPLED_REGION_COUNT);
    long scanTimeout = this.config.getLong(HConstants.HEALTH_SCAN_TIMEOUT, HConstants.DEFAULT_HEALTH_SCAN_TIMEOUT);
    String currentServerName = "localhost";
    if (stopper instanceof HRegionServer) {
      currentServerName = ((HRegionServer)stopper).getServerName().getServerName();
    }
    this.threshold = config.getInt(HConstants.HEALTH_FAILURE_THRESHOLD,
      HConstants.DEFAULT_HEALTH_FAILURE_THRESHOLD);
    this.failureWindow = (long)this.threshold * (long)sleepTime;
    if(conf.getBoolean(HConstants.HEALTH_DIRECT_CHECK, false)){
      this.rs = (HRegionServer)stopper;
      long directTimeout = this.config.getLong(HConstants.HEALTH_DIRECTCHECK_TIMEOUT,
              HConstants.DEFAULT_HEALTH_DIRECTCHECK_TIMEOUT);
      healthChecker = new DirectHealthChecker();
      ((DirectHealthChecker)healthChecker).init(directTimeout, threadPoolSize, failedThreshold,
              sampledRegionCount, scanTimeout, currentServerName, (HRegionServer)stopper, this.config);
    }else{
      String healthCheckScript = this.config.get(HConstants.HEALTH_SCRIPT_LOC);
      long scriptTimeout = this.config.getLong(HConstants.HEALTH_SCRIPT_TIMEOUT,
              HConstants.DEFAULT_HEALTH_SCRIPT_TIMEOUT);
      healthChecker = new HealthChecker();
      healthChecker.init(healthCheckScript, scriptTimeout,
              new Integer(threadPoolSize),
              new Float(failedThreshold),
              new Integer(sampledRegionCount),
              new Long(scanTimeout),
              currentServerName);
    }
  }

  @Override
  protected void chore() {
    HealthReport report = healthChecker.checkHealth();
    boolean isHealthy = (report.getStatus() == HealthCheckerExitStatus.SUCCESS);
    if (!isHealthy) {
      boolean needToStop = decideToStop();
      directHealthCheckNumUnhealthy++;
      if(rs != null){
        this.rs.setDirectHealthCheckNumUnhealthy(directHealthCheckNumUnhealthy);
      }
      if (needToStop) {
        LOG.info("The  node reported unhealthy " + threshold + " number of times consecutively.");
        if(!config.getBoolean(HConstants.HEALTH_DIRECT_CHECK, false)) getStopper().stop(
          "The  node reported unhealthy " + threshold + " number of times consecutively.");
      }
    }
    // Always log health report.
    LOG.info("Health status at " +  (new Date()) + " : "
            + report.getHealthReport());
  }

  private boolean decideToStop() {
    boolean stop = false;
    if (numTimesUnhealthy == 0) {
      // First time we are seeing a failure. No need to stop, just
      // record the time.
      numTimesUnhealthy++;
      startWindow = System.currentTimeMillis();
    } else {
      if ((System.currentTimeMillis() - startWindow) < failureWindow) {
        numTimesUnhealthy++;
        if (numTimesUnhealthy == threshold) {
          stop = true;
        } else {
          stop = false;
        }
      } else {
        // Outside of failure window, so we reset to 1.
        numTimesUnhealthy = 1;
        startWindow = System.currentTimeMillis();
        stop = false;
      }
    }

    return stop;
  }

  @Override
  public void cancel(boolean mayInterruptIfRunning) {
    if(rs != null){
      this.rs.setDirectHealthCheckFailedRegionCount(0);
      this.rs.setDirectHealthCheckSelectedRegionCount(0);
      this.rs.setDirectHealthCheckFailedRatio(0);
    }
    super.cancel(mayInterruptIfRunning);
  }

}
