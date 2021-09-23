package com.google.cloud.bigtable.hbase.replication;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudBigtableTableReplicatorFactory {

  // The cache of tableReplicator singletons.
  private static final Map<String, CloudBigtableTableReplicator> INSTANCE = new HashMap<>();

  private static Connection connection = null;
  // Explicit lock object, hidden from outside.
  private static final Object LOCK = new Object();
  private static final Logger LOG =
      LoggerFactory.getLogger(CloudBigtableTableReplicatorFactory.class);

  public static CloudBigtableTableReplicator getReplicator(
      String tableName, String projectId, String instanceId) throws IOException {
    // Guard against concurrent access
    if (connection == null) {
      synchronized (LOCK) {
        // Double checked locking
        if (connection == null) {
          LOG.error("Connecting to " + projectId + ":" + instanceId);
          connection = BigtableConfiguration.connect(projectId, instanceId);
        } else {
          LOG.error("Re-using connection to " + projectId + ":" + instanceId);
        }
      }
    }
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
  // TODO: Maybe insert a shutdown hook and call connection.close() there.
}
