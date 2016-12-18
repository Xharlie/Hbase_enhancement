package org.apache.hadoop.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.tool.DirectHealthCheck;
import org.apache.hadoop.util.Shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xharlie on 8/12/16.
 */
public class DirectHealthChecker extends HealthChecker {
  private String exceptionStackTrace;
  private static Log LOG = LogFactory.getLog(DirectHealthChecker.class);
  private long directCheckTimeout;
  private DirectHealthCheck directHealthCheck;
  private volatile HRegionServer rs;

  /**
   * Initialize.
   *
   * @param configuration
   */
  public void init(long timeout, int threadPoolSize, float failedThreshold,
                   int sampledRegionCount, long opTimeout, String currentServerName, HRegionServer rs, Configuration conf) {
    this.directCheckTimeout = timeout;
    this.rs = rs;
    directHealthCheck = new DirectHealthCheck(threadPoolSize, failedThreshold,
            sampledRegionCount, opTimeout, currentServerName, rs, conf);
    LOG.info("HealthChecker initialized as Direct Health Checker, timeout=" + timeout);
  }

  @Override
  public HealthReport checkHealth() {
    HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
    boolean success = true;
    try {
      // Calling this execute leaves around running executor threads.
      success = directHealthCheck.execute(rs.getOnlineRegions());
    } catch (IOException e) {
      LOG.warn("Caught exception : " + e);
      status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
      exceptionStackTrace = org.apache.hadoop.util.StringUtils.stringifyException(e);
    } finally {
      if (status == HealthCheckerExitStatus.SUCCESS) {
        if (!success) {
          status = HealthCheckerExitStatus.FAILED;
        }
      }
      if (!success) {
        this.rs.setDirectHealthCheckFailedRegionCount(0);
        this.rs.setDirectHealthCheckSelectedRegionCount(0);
        this.rs.setDirectHealthCheckFailedRatio(1);
      }
    }
    return new HealthReport(status, getHealthReport(status));
  }

  @VisibleForTesting
  public DirectHealthCheck getDirectHealthCheck() {
    return this.directHealthCheck;
  }

  @VisibleForTesting
  public boolean checkHealthFromOutSide() throws IOException {
    return directHealthCheck.execute(rs.getOnlineRegions());
  }

  @VisibleForTesting
  public boolean checkHealthFromOutSide(List<Region> regions) throws IOException {
    return directHealthCheck.execute(regions);
  }

  private String getHealthReport(HealthCheckerExitStatus status) {
    String healthReport = null;
    switch (status) {
      case SUCCESS:
        healthReport = "Server is healthy.";
        break;
      case FAILED_WITH_EXCEPTION:
        healthReport = exceptionStackTrace;
        break;
      case FAILED:
        healthReport = "number of failed regions goes beyond threshold";
        break;
    }
    return healthReport;
  }
}
