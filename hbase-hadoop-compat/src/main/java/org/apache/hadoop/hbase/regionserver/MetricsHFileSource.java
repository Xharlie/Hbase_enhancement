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

package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * Interface for classes that expose metrics about HFile.
 */
public interface MetricsHFileSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "HFile";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HFile of regions managed by HBase RegionServer";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  /**
   * Update the fs positional read time histogram
   * @param t time it took, in milliseconds
   * @param pread true for positional read, false for sequential read
   */
  void updateFsReadTime(long t, boolean pread);

  /**
   * Update the fs write time histogram
   * @param t time it took, in milliseconds
   */
  void updateFsWriteTime(long t);

  // Strings used for exporting to metrics system.
  String FS_READ_KEY = "fsReadTime";
  String FS_PREAD_KEY = "fsPReadTime";
  String FS_WRITE_KEY = "fsWriteTime";
}
