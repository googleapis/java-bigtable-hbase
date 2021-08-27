package com.google.cloud.bigtable.hbase.replication;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.Connection;

public class CloudBigtableTableReplicatorFactory {

  // The cache of tableReplicator singletons.
  private static final Map<String, CloudBigtableTableReplicator> INSTANCE = new HashMap<>();

  // Explicit lock object, hidden from outside.
  private static final Object LOCK = new Object();

  public static CloudBigtableTableReplicator getReplicator(
      String tableName, Connection connection) {
    // Guard against concurrent access
    if (!INSTANCE.containsKey(tableName)) {
      synchronized (LOCK) {
        // Double checked locking.
        if (!INSTANCE.containsKey(tableName)) {
          INSTANCE.put(tableName, new CloudBigtableTableReplicator(tableName, connection));
        }
      }
    }
    return INSTANCE.get(tableName);
  }
}
