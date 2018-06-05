/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.metrics;

import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class TestBigtableClientMetrics {

  private MetricRegistry originalMetricRegistry;
  private MetricLevel originalLevelToLog;

  @Mock
  MetricRegistry mockMetricRegistry;

  @Mock
  Timer mockTimer;

  @Mock
  Meter mockMeter;

  @Mock
  Counter mockCounter;

  @Before
  public void setup() {
    originalLevelToLog = BigtableClientMetrics.getLevelToLog();
    originalMetricRegistry = BigtableClientMetrics.getMetricRegistry(originalLevelToLog);
    MockitoAnnotations.initMocks(this);
    when(mockMetricRegistry.timer(any(String.class))).thenReturn(mockTimer);
    when(mockMetricRegistry.meter(any(String.class))).thenReturn(mockMeter);
    when(mockMetricRegistry.counter(any(String.class))).thenReturn(mockCounter);
  }
  
  @After
  public void teardown() {
    BigtableClientMetrics.setMetricRegistry(originalMetricRegistry);
    BigtableClientMetrics.setLevelToLog(originalLevelToLog);
  }

  @Test
  public void testMetricLevels_Info() {
    testLevel(MetricLevel.Info, MetricLevel.Info);
  }

  @Test
  public void testMetricLevels_Debug() {
    testLevel(MetricLevel.Debug, MetricLevel.Info, MetricLevel.Debug);
  }

  @Test
  public void testMetricLevels_Trace() {
    testLevel(MetricLevel.Trace, MetricLevel.Info, MetricLevel.Debug, MetricLevel.Trace);
  }

  private void testLevel(MetricLevel level, MetricLevel... goodLevels) {
    BigtableClientMetrics.setMetricRegistry(mockMetricRegistry);
    BigtableClientMetrics.setLevelToLog(level);
    List<MetricLevel> acceptableLevels = Arrays.asList(goodLevels);
    for (MetricLevel metricLevel : MetricLevel.values()) {
      MetricRegistry foundRegistry = BigtableClientMetrics.getMetricRegistry(metricLevel);
      if (acceptableLevels.contains(metricLevel)) {
        Assert.assertSame(mockMetricRegistry, foundRegistry);
        Assert.assertSame(mockTimer, BigtableClientMetrics.timer(metricLevel, "any"));
        Assert.assertSame(mockMeter, BigtableClientMetrics.meter(metricLevel, "any"));
        Assert.assertSame(mockCounter, BigtableClientMetrics.counter(metricLevel, "any"));
      } else {
        Assert.assertSame(MetricRegistry.NULL_METRICS_REGISTRY, foundRegistry);
      }
    }
  }
}

