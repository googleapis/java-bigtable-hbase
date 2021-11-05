package com.google.cloud.bigtable.hbase.replication;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Make it singleton
public class CloudBigtableTableReplicatorFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(CloudBigtableTableReplicatorFactory.class);
  /**
   * A table name to table replicator map. This map is maintained per Factory object (which in turn
   * is maintained per {@link HbaseToCloudBigtableReplicationEndpoint}). It is inexpensive to create
   * a TableReplicator using the same shared connection.
   * {@link HbaseToCloudBigtableReplicationEndpoint} makes sure to re-use the connection, so here we
   * can keep a copy of TableReplicator per endpoint.
    */
  private final Map<String, CloudBigtableTableReplicator> tableReplicatorMap = new HashMap<>();

  /**
   * Shared connection owned by {@link HbaseToCloudBigtableReplicationEndpoint} class. Do not close
   * this connection in this class.
   */
  private final Connection connection;

  public CloudBigtableTableReplicatorFactory(Connection connection) {

    // TODO Delete debugging code
    LOG.debug("Creating a TableReplicatorFactory!");
    this.connection = connection;
  }

  public synchronized CloudBigtableTableReplicator getReplicator(
      String tableName) throws IOException {
    // Guard against concurrent access
    if (!tableReplicatorMap.containsKey(tableName)) {
      tableReplicatorMap.put(tableName, new CloudBigtableTableReplicator(tableName, connection));
    }
    return tableReplicatorMap.get(tableName);
  }
}
