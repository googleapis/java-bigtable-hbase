/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.SyncTable.SyncMapper;

/** A helper class to access package private fields of SyncTable. */
@InternalApi
public class BigtableSyncTableAccessor {

  // Restrict object creation. This class should only be used as accessor for SyncTable
  private BigtableSyncTableAccessor() {}

  public static String getSourceZkCluster(SyncTable syncTable) {
    return syncTable.sourceZkCluster;
  }

  public static void setSourceZkCluster(SyncTable syncTable, String sourceZkCluster) {
    syncTable.sourceZkCluster = sourceZkCluster;
  }

  public static Path getSourceHashDir(SyncTable syncTable) {
    return syncTable.sourceHashDir;
  }

  public static void setSourceHashDir(SyncTable syncTable, Path sourceHashDir) {
    syncTable.sourceHashDir = sourceHashDir;
  }

  public static String getSourceTableName(SyncTable syncTable) {
    return syncTable.sourceTableName;
  }

  public static void setSourceTableName(SyncTable syncTable, String sourceTableName) {
    syncTable.sourceTableName = sourceTableName;
  }

  public static String getTargetZkCluster(SyncTable syncTable) {
    return syncTable.targetZkCluster;
  }

  public static void setTargetZkCluster(SyncTable syncTable, String targetZkCluster) {
    syncTable.targetZkCluster = targetZkCluster;
  }

  public static String getTargetTableName(SyncTable syncTable) {
    return syncTable.targetTableName;
  }

  public static void setTargetTableName(SyncTable syncTable, String targetTableName) {
    syncTable.targetTableName = targetTableName;
  }

  public static String getConfDryRunKey() {
    return SyncTable.DRY_RUN_CONF_KEY;
  }

  public static void setDryRun(SyncTable syncTable, boolean dryRun) {
    syncTable.dryRun = dryRun;
  }

  public static void setDoDeletes(SyncTable syncTable, boolean doDeletes) {
    syncTable.doDeletes = doDeletes;
  }

  public static void setDoPuts(SyncTable syncTable, boolean doPuts) {
    syncTable.doPuts = doPuts;
  }

  public static void setIgnoreTimestamps(SyncTable syncTable, boolean ignoreTimestamps) {
    syncTable.ignoreTimestamps = ignoreTimestamps;
  }

  public static Connection getSourceConnection(SyncMapper mapper) {
    return mapper.sourceConnection;
  }

  public static Table getSourceTable(SyncMapper mapper) {
    return mapper.sourceTable;
  }

  public static Connection getTargetConnection(SyncMapper mapper) throws IOException {
    return mapper.targetConnection;
  }

  public static Table getTargetTable(SyncMapper mapper) {
    return mapper.targetTable;
  }

  public static String getTargetZkClusterConf(Configuration conf) {
    return conf.get(SyncTable.TARGET_ZK_CLUSTER_CONF_KEY);
  }

  public static String getSourceZkClusterConf(Configuration conf) {
    return conf.get(SyncTable.SOURCE_ZK_CLUSTER_CONF_KEY);
  }

  public static void setTargetZkClusterConf(Configuration conf, String targetZkClusterConf) {
    conf.set(SyncTable.TARGET_ZK_CLUSTER_CONF_KEY, targetZkClusterConf);
  }

  public static void setSourceZkClusterConf(Configuration conf, String sourceZkClusterConf) {
    conf.set(SyncTable.SOURCE_ZK_CLUSTER_CONF_KEY, sourceZkClusterConf);
  }

  public static Connection setSourceConnection(SyncMapper mapper, Configuration conf)
      throws IOException {
    mapper.sourceConnection = ConnectionFactory.createConnection(conf);
    return mapper.sourceConnection;
  }

  public static void setSourceTable(SyncMapper mapper, Connection connection, Configuration conf)
      throws IOException {
    mapper.sourceTable =
        connection.getTable(TableName.valueOf(conf.get(SyncTable.SOURCE_TABLE_CONF_KEY)));
  }

  public static Connection setTargetConnection(SyncMapper mapper, Configuration conf)
      throws IOException {
    mapper.targetConnection = ConnectionFactory.createConnection(conf);
    return mapper.targetConnection;
  }

  public static void setTargetTable(SyncMapper mapper, Connection connection, Configuration conf)
      throws IOException {
    mapper.targetTable =
        connection.getTable(TableName.valueOf(conf.get(SyncTable.TARGET_TABLE_CONF_KEY)));
  }
}
