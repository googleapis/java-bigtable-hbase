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
package com.google.cloud.bigtable.mapreduce.validation;

import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.SOURCE_BT_APP_PROFILE_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.SOURCE_BT_INSTANCE_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.SOURCE_BT_PROJECTID_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.TARGET_BT_APP_PROFILE_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.TARGET_BT_INSTANCE_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.TARGET_BT_PROJECTID_CONF_KEY;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.BigtableSyncTableAccessor;
import org.apache.hadoop.hbase.mapreduce.SyncTable.SyncMapper;
import org.apache.hadoop.mapreduce.Mapper;

/** Bigtable SyncMapper */
public class BigtableSyncMapper extends SyncMapper {

  private static final Log LOG = LogFactory.getLog(BigtableSyncMapper.class);

  @Override
  public void setup(
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation>.Context context)
      throws IOException {

    // modify configuration to establish valid connection
    Configuration conf = context.getConfiguration();
    conf.unset(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL);

    // required setup for SyncMapper with Bigtable as source or target
    setupForParentSyncMapper(context);

    // override existing target connection and establish connection to bigtable
    if (!conf.onlyKeyExists(TARGET_BT_PROJECTID_CONF_KEY)) {
      createBigtableTargetConnection(conf);
    }

    // override existing source connection and establish connection to bigtable
    if (!conf.onlyKeyExists(SOURCE_BT_PROJECTID_CONF_KEY)) {
      createBigtableSourceConnection(conf);
    }
  }

  /**
   * Method that prepares configuration for SyncTable.SyncMapper.super(context). SyncTable
   * initializes map task configuration and connections with source and target. A valid source and
   * target are required for initialization and a temporary connection is established if Bigtable is
   * configured as a source or as a target. The connection is not used in the setup and only
   * required for initialization.
   *
   * @param context
   */
  private void setupForParentSyncMapper(Context context) throws IOException {
    Configuration conf = context.getConfiguration();

    // since both source/target require establishing a connection in super.setup(), set the source
    // as the target or target as source to mock a connection if bigtable is configured at either
    // side of the sync.
    String targetZkClusterConf = conf.get(BigtableSyncTableAccessor.getTargetZkClusterConfKey());
    String sourceZkClusterConf = conf.get(BigtableSyncTableAccessor.getSourceZkClusterConfKey());
    if (null == targetZkClusterConf && null != sourceZkClusterConf) {
      conf.set(BigtableSyncTableAccessor.getTargetZkClusterConfKey(), sourceZkClusterConf);
      LOG.info(
          "target connection temporarily set as source for initialization only: "
              + sourceZkClusterConf);
    }

    if (null == sourceZkClusterConf && null != targetZkClusterConf) {
      conf.set(BigtableSyncTableAccessor.getSourceZkClusterConfKey(), targetZkClusterConf);
      LOG.info(
          "source connection temporarily set as target for initialization only: "
              + targetZkClusterConf);
    }

    super.setup(context);
  }

  /**
   * create source connection with Bigtable
   *
   * @param conf
   * @throws IOException
   */
  private void createBigtableSourceConnection(Configuration conf) throws IOException {
    // re-create connection with bigtable
    closeConnection(BigtableSyncTableAccessor.getSourceConnection(this), "source");

    // create source connection config
    // inherit base config, but override connection based on job args
    Configuration bigtableConf =
        getBigtableConfiguration(
            new Configuration(conf),
            SOURCE_BT_PROJECTID_CONF_KEY,
            SOURCE_BT_INSTANCE_CONF_KEY,
            SOURCE_BT_APP_PROFILE_CONF_KEY,
            "source");
    Connection srcConn =
        BigtableSyncTableAccessor.setSourceConnection(
            this, ConnectionFactory.createConnection(bigtableConf));
    TableName tableName =
        TableName.valueOf(bigtableConf.get(BigtableSyncTableAccessor.getSourceTableConfKey()));
    Table table = srcConn.getTable(tableName);
    BigtableSyncTableAccessor.setSourceTable(this, table);
  }

  /**
   * create target connection with Bigtable
   *
   * @param conf
   * @throws IOException
   */
  private void createBigtableTargetConnection(Configuration conf) throws IOException {
    // re-create connection with bigtable
    closeConnection(BigtableSyncTableAccessor.getTargetConnection(this), "target");

    // create target connection config
    // inherit base config, but override connection based on job args
    Configuration bigtableConf =
        getBigtableConfiguration(
            new Configuration(conf),
            TARGET_BT_PROJECTID_CONF_KEY,
            TARGET_BT_INSTANCE_CONF_KEY,
            TARGET_BT_APP_PROFILE_CONF_KEY,
            "target");
    Connection targetConn =
        BigtableSyncTableAccessor.setTargetConnection(
            this, ConnectionFactory.createConnection(bigtableConf));
    TableName tableName =
        TableName.valueOf(bigtableConf.get(BigtableSyncTableAccessor.getTargetTableConfKey()));
    Table table = targetConn.getTable(tableName);
    BigtableSyncTableAccessor.setTargetTable(this, table);
  }

  /**
   * close connection
   *
   * @param conn
   * @param sourceOrTarget
   */
  private void closeConnection(Connection conn, String sourceOrTarget) throws IOException {
    try {
      if (conn != null) {
        conn.close();
      }
    } catch (IOException ioe) {
      LOG.warn("error closing temporary " + sourceOrTarget + " connection, " + ioe.getMessage());
      throw ioe;
    }
  }

  /**
   * create bigtable configuration
   *
   * @param conf
   * @param projectIdKey
   * @param instanceKey
   * @param appProfileKey
   * @param sourceOrTarget
   * @return
   */
  private Configuration getBigtableConfiguration(
      Configuration conf,
      String projectIdKey,
      String instanceKey,
      String appProfileKey,
      String sourceOrTarget) {
    Configuration bigtableConf = new Configuration(conf);

    BigtableConfiguration.configure(
        bigtableConf,
        bigtableConf.get(projectIdKey),
        bigtableConf.get(instanceKey),
        bigtableConf.get(appProfileKey, ""));
    bigtableConf.set(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "HBaseMRSyncTable");

    LOG.info(
        "configuring "
            + sourceOrTarget
            + " connection to Bigtable: "
            + bigtableConf.get(projectIdKey)
            + ", "
            + bigtableConf.get(instanceKey)
            + ", "
            + bigtableConf.get(appProfileKey, ""));

    return bigtableConf;
  }
}
