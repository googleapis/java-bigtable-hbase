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

package com.google.cloud.bigtable.hbase.replication.adapters;

import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;

@InterfaceAudience.Private
public class IncompatibleMutationAdapterFactory {

  private final Configuration conf;
  private final MetricsExporter metricsExporter;
  private final Connection connection;

  public IncompatibleMutationAdapterFactory(
      Configuration conf, MetricsExporter metricsExporter, Connection connection) {
    this.conf = conf;
    this.metricsExporter = metricsExporter;
    this.connection = connection;
  }

  public IncompatibleMutationAdapter createIncompatibleMutationAdapter() {
    // TODO Initialize from the config key
    return new ApproximatingIncompatibleMutationAdapter(conf, metricsExporter, connection);
  }
}
