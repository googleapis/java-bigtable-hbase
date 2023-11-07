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

package com.google.cloud.bigtable.hbase1_x.replication.metrics;


import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import java.lang.reflect.Method;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint.Context;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** HBaseMetricsExporter implements MetricExporter which bridges with MetricsSource. */
@InterfaceAudience.Private
public class HBaseMetricsExporter implements MetricsExporter {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseMetricsExporter.class);

  // Force the use of static factory method to create instances.
  protected HBaseMetricsExporter() {}

  // same pattern as used by HbaseInterClusterRepl
  private MetricsSource metricsSource;

  public void init(Context ctx) {
    this.metricsSource = ctx.getMetrics();
  }

  @Override
  public void incCounters(String counterName, long delta) {
    metricsSource.incCounters(counterName, delta);
  }

  /**
   * Creates the right implementation for HBaseMetricsExporter. The incCounter method used to
   * integrate with HBase metrics was introduced in HBase 1.4, so when running on HBase version 1.3
   * or lower, we need to skip incrementing the counters. More details on:
   * https://github.com/googleapis/java-bigtable-hbase/issues/3596
   */
  public static HBaseMetricsExporter create() {
    // TODO: Define configuration that allows users to inject a custom implementation of
    //  HBaseMetricsExporter
    try {
      Method method = MetricsSource.class.getMethod("incCounters", String.class, long.class);
      // HBase version > 1.4, supports incCounters, return normal MetricsExporter.
      return new HBaseMetricsExporter();
    } catch (NoSuchMethodException e) {
      // HBase version <1.4 : HBase does not support generic counters. Revert to no-op metrics
      // exporter.
      LOG.warn(
          "Can not find MetricsSource.incCounters method, probably running HBase 1.3 or older."
              + " Disabling metrics for IncompatibleMutations. Please refer to "
              + "https://github.com/googleapis/java-bigtable-hbase/issues/3596 for details.");
      return new NoOpHBaseMetricsExporter();
    }
  }
}
