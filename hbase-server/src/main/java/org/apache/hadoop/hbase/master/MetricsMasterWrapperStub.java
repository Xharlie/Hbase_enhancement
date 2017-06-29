package org.apache.hadoop.hbase.master;

public class MetricsMasterWrapperStub implements MetricsMasterWrapper {

  @Override
  public String getServerName() {
    return "test";
  }

  @Override
  public double getAverageLoad() {
    return 2.0;
  }

  @Override
  public String getClusterId() {
    return "testCluster";
  }

  @Override
  public String getZookeeperQuorum() {
    return "testQuorum";
  }

  @Override
  public String[] getCoprocessors() {
    return null;
  }

  @Override
  public long getStartTime() {
    return 1496287597;
  }

  @Override
  public long getActiveTime() {
    return 1496287697;
  }

  @Override
  public boolean getIsActiveMaster() {
    return false;
  }

  @Override
  public String getRegionServers() {
    return "a;b;c";
  }

  @Override
  public int getNumRegionServers() {
    return 3;
  }

  @Override
  public String getDeadRegionServers() {
    return "d;e";
  }

  @Override
  public int getNumDeadRegionServers() {
    return 2;
  }

}
