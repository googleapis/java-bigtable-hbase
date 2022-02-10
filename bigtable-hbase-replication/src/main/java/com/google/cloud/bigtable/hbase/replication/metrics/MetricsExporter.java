package com.google.cloud.bigtable.hbase.replication.metrics;

/**
 * MetricsExporter is exposed as an interface to remove dependency on MetricsSource which is in hbase-server.
 */
public interface MetricsExporter {
    void incCounters(String var1, long var2);
}
