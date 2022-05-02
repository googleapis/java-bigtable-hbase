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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hbase.replication.ReplicationEndpoint.Context;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class HBaseMetricsExporterTest {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock Context context;
  @Mock MetricsSource metricsSource;

  HBaseMetricsExporter hbaseMetricsExporter;

  public static final String METRIC_EXAMPLE_A = "exampleA";
  public static final String METRIC_EXAMPLE_B = "exampleB";

  @Before
  public void setUp() {
    when(context.getMetrics()).thenReturn(metricsSource);
    hbaseMetricsExporter = HBaseMetricsExporter.create();
    hbaseMetricsExporter.init(context);
  }

  @After
  public void tearDown(){
    reset(context, metricsSource);
  }

  @Test
  public void testCreate(){
    // Make sure that create returns an HBaseMetricsExporter object. There is no good way to test
    // the other case where incCounter method is not available as it requires adding hbase <1.4 to
    // the classpath along with hbase 1.4.
    assertEquals(HBaseMetricsExporter.class, hbaseMetricsExporter.getClass());
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
