package com.google.cloud.bigtable.hbase.replication.adapters;

import com.google.cloud.bigtable.hbase.replication.metrics.MetricsExporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public class IncompatibleMutationAdapterFactory {

  private static final String INCOMPATIBLE_MUTATION_ADAPTER_CLASS_KEY =
      "google.bigtable.incompatible_mutation.adapter.class";

  private final Configuration conf;
  private final MetricsExporter metricsExporter;
  private final Connection connection;

  public IncompatibleMutationAdapterFactory(Configuration conf, MetricsExporter metricsExporter,
      Connection connection) {
    this.conf = conf;
    this.metricsExporter = metricsExporter;
    this.connection = connection;
  }

  public IncompatibleMutationAdapter createIncompatibleMutationAdapter() {
    // TODO Initialize from the config key
    return new ApproximatingIncompatibleMutationAdapter(conf, metricsExporter, connection);
  }
}
