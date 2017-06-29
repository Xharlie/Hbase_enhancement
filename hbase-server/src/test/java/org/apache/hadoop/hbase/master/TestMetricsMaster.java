/**
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit test version of master metrics tests.
 */
@Category(SmallTests.class)
public class TestMetricsMaster {
  public static MetricsAssertHelper HELPER =
      CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private MetricsMaster msm;
  private MetricsMasterSource serverSource;

  @BeforeClass
  public static void classSetUp() {
    HELPER.init();
  }

  @Before
  public void setUp() {
    msm = new MetricsMaster(new MetricsMasterWrapperStub());
    serverSource = msm.getMetricsSource();
  }

  @Test
  public void testConstuctor() {
    assertNotNull("There should be a hadoop1/hadoop2 metrics source", msm.getMetricsSource());
  }

  @Test
  public void testWrapperSource() {
    HELPER.assertTag("clusterId", "testCluster", serverSource);
    HELPER.assertTag("serverName", "test", serverSource);
    HELPER.assertTag("zookeeperQuorum", "testQuorum", serverSource);
    HELPER.assertTag("isActiveMaster", "false", serverSource);
    HELPER.assertTag("liveRegionServers", "a;b;c", serverSource);
    HELPER.assertTag("deadRegionServers", "d;e", serverSource);
    HELPER.assertGauge("averageLoad", 2.0, serverSource);
    HELPER.assertGauge("masterStartTime", 1496287597, serverSource);
    HELPER.assertGauge("masterActiveTime", 1496287697, serverSource);
    HELPER.assertGauge("numRegionServers", 3, serverSource);
    HELPER.assertGauge("numDeadRegionServers", 2, serverSource);
  }

  @Test
  public void testOldWALCount() {
    msm.updateOldWALNumber(100);
    HELPER.assertCounter("oldWALNumber", 100, serverSource);

    msm.updateOldWALNumber(200);
    HELPER.assertCounter("oldWALNumber", 200, serverSource);

    msm.updateOldWALNumber(0);
    HELPER.assertCounter("oldWALNumber", 0, serverSource);
  }
}
