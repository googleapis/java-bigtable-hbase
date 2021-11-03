package com.google.cloud.bigtable.hbase.replication;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Make it singleton
public class CloudBigtableTableReplicatorFactory {

  // The cache of tableReplicator singletons. This is shared by all the instances of the factory.
  private final Map<String, CloudBigtableTableReplicator> INSTANCE = new HashMap<>();

  private final Connection connection;

  public CloudBigtableTableReplicatorFactory(Connection connection) {
    this.connection = connection;
  }

  public synchronized CloudBigtableTableReplicator getReplicator(
      String tableName) throws IOException {
    // Guard against concurrent access
    if (!INSTANCE.containsKey(tableName)) {
      INSTANCE.put(tableName, new CloudBigtableTableReplicator(tableName, connection));
    }
    return INSTANCE.get(tableName);
  }
// TODO: Maybe insert a shutdown hook and call connection.close() there.
}
