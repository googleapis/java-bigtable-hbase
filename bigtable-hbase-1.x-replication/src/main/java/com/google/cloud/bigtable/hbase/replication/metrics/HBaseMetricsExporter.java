package com.google.cloud.bigtable.hbase.replication.metrics;

import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;

/**
 * HBaseMetricsExporter implements MetricExporter which bridges with MetricsSource.
 */
public class HBaseMetricsExporter implements MetricsExporter {
    // same pattern as used by HbaseInterClusterRepl
    private MetricsSource metricsSource;

    public void setMetricsSource(MetricsSource metricsSource) {
        this.metricsSource = metricsSource;
    }

    @Override
    public void incCounters(String var1, long var2) {
        metricsSource.incCounters(var1, var2);
    }
}
