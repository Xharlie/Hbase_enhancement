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
package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.controller.ThroughputController;
import org.apache.hadoop.util.ReflectionUtils;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class CompactionThroughputControllerFactory {

  private static final Log LOG = LogFactory.getLog(CompactionThroughputControllerFactory.class);

  public static final String HBASE_THROUGHPUT_CONTROLLER_KEY =
      "hbase.regionserver.throughput.controller";

  private static final Class<? extends ThroughputController>
      DEFAULT_THROUGHPUT_CONTROLLER_CLASS = NoLimitCompactionThroughputController.class;

  public static ThroughputController create(RegionServerServices server,
      Configuration conf) {
    Class<? extends ThroughputController> clazz = getThroughputControllerClass(conf);
    ThroughputController controller = ReflectionUtils.newInstance(clazz, conf);
    controller.setup(server);
    return controller;
  }

  public static Class<? extends ThroughputController> getThroughputControllerClass(
      Configuration conf) {
    String className =
        conf.get(HBASE_THROUGHPUT_CONTROLLER_KEY, DEFAULT_THROUGHPUT_CONTROLLER_CLASS.getName());
    try {
      return Class.forName(className).asSubclass(ThroughputController.class);
    } catch (Exception e) {
      LOG.warn(
        "Unable to load configured throughput controller '" + className
            + "', load default throughput controller "
            + DEFAULT_THROUGHPUT_CONTROLLER_CLASS.getName() + " instead", e);
      return DEFAULT_THROUGHPUT_CONTROLLER_CLASS;
    }
  }
}
