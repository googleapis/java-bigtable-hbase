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

/**
 * No-op implemnentation of MetricsExporter interface. To be used where incCounters method from
 * HBase MetricsSource is not available.
 */
class NoOpHBaseMetricsExporter extends HBaseMetricsExporter {

  // Use the HBaseMetricsExporter.create method to create instances of this class.
  NoOpHBaseMetricsExporter() {}

  @Override
  public void incCounters(String counterName, long delta) {}
}
