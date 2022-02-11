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

package com.google.cloud.bigtable.hbase.replication.metrics;

import static org.mockito.Mockito.*;

import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class HBaseMetricsExporterTest {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock MetricsSource metricsSource;

  HBaseMetricsExporter hbaseMetricsExporter;

  public static final String METRIC_EXAMPLE_A = "exampleA";
  public static final String METRIC_EXAMPLE_B = "exampleB";

  @Before
  public void setUp() {
    hbaseMetricsExporter = new HBaseMetricsExporter();
    hbaseMetricsExporter.setMetricsSource(metricsSource);
  }

  @Test
  public void testMetricsPropagation() {
    hbaseMetricsExporter.incCounters(METRIC_EXAMPLE_A, 10);
    hbaseMetricsExporter.incCounters(METRIC_EXAMPLE_B, 10);
    verify(metricsSource, times(1)).incCounters(METRIC_EXAMPLE_A, 10);
    verify(metricsSource, times(1)).incCounters(METRIC_EXAMPLE_B, 10);
    // increment metric B again
    hbaseMetricsExporter.incCounters(METRIC_EXAMPLE_B, 10);
    verify(metricsSource, times(2)).incCounters(METRIC_EXAMPLE_B, 10);
  }
}
