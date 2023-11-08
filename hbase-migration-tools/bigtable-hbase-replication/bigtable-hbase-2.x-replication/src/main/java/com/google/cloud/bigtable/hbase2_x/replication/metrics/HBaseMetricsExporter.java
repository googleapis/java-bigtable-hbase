/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.bigtable.hbase2_x.replication.metrics;

import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.yetus.audience.InterfaceAudience;

/** HBaseMetricsExporter implements MetricExporter which bridges with MetricsSource. */
@InterfaceAudience.Private
public class HBaseMetricsExporter implements MetricsExporter {
  private MetricsSource metricsSource;

  public void setMetricsSource(MetricsSource metricsSource) {
    this.metricsSource = metricsSource;
  }

  @Override
  public void incCounters(String counterName, long delta) {
    metricsSource.incCounters(counterName, delta);
  }
}
