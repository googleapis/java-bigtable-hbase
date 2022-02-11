package com.google.cloud.bigtable.hbase.replication.metrics;

import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public class HBaseMetricsExporterTest {
    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    MetricsSource metricsSource;

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