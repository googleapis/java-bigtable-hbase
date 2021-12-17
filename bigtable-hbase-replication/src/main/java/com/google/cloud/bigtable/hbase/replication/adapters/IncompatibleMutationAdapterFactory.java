package com.google.cloud.bigtable.hbase.replication.adapters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;

public class IncompatibleMutationAdapterFactory {

  private static final String INCOMPATIBLE_MUTATION_ADAPTER_CLASS_KEY =
      "google.bigtable.incompatible_mutation.adapter.class";

  private final Configuration conf;
  private final MetricsSource metricsSource;
  private final Connection connection;

  public IncompatibleMutationAdapterFactory(Configuration conf, MetricsSource metricsSource,
      Connection connection) {
    this.conf = conf;
    this.metricsSource = metricsSource;
    this.connection = connection;
  }

  public IncompatibleMutationAdapter createIncompatibleMutationAdapter() {
    // TODO Initialize from the config key
    return new ApproximatingIncompatibleMutationAdapter(conf, metricsSource, connection);
  }
}
