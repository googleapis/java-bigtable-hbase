package com.google.cloud.bigtable.hbase1_x.replication.metrics;

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;

/**
 * No-op implemnentation of MetricsExporter interface. To be used where incCounters method from
 * HBase MetricsSource is not available.
 */
@InternalApi
public class NoOpHBaseMetricsExporter extends HBaseMetricsExporter {

  // Use the HBaseMetricsExporter.create method to create instances of this class.
  @InternalApi
  NoOpHBaseMetricsExporter() {}

  @Override
  public void incCounters(String counterName, long delta) {}
}
