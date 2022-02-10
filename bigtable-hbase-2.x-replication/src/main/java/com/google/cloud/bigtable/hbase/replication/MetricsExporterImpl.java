package com.google.cloud.bigtable.hbase.replication;

import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
/**
 * MetricsExporterImpl implements MetricExporter which bridges with MetricsSource.
 */
public class MetricsExporterImpl implements MetricsExporter {
    // set this as static
    private static MetricsSource metricsSource;

    public void setMetricsSource(MetricsSource metricsSource) {
        this.metricsSource = metricsSource;
    }

    @Override
    public void incCounters(String var1, long var2) {
        metricsSource.incCounters(var1, var2);
    }
}
