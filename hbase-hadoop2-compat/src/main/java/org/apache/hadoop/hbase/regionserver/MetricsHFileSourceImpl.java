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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;

/**
 * Hadoop2 implementation of {@link MetricsHFileSource}.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsHFileSourceImpl extends BaseSourceImpl implements MetricsHFileSource {
  private final MetricHistogram fsReadTimeHisto;
  private final MetricHistogram fsPReadTimeHisto;
  private final MetricHistogram fsWriteTimeHisto;

  public MetricsHFileSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsHFileSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.fsReadTimeHisto = getMetricsRegistry().newHistogram(FS_READ_KEY);
    this.fsPReadTimeHisto = getMetricsRegistry().newHistogram(FS_PREAD_KEY);
    this.fsWriteTimeHisto = getMetricsRegistry().newHistogram(FS_WRITE_KEY);
  }

  @Override
  public void updateFsReadTime(long t, boolean pread) {
    if (pread) {
      this.fsPReadTimeHisto.add(t);
    } else {
      this.fsReadTimeHisto.add(t);
    }
  }

  @Override
  public void updateFsWriteTime(long t) {
    this.fsWriteTimeHisto.add(t);
  }

}
