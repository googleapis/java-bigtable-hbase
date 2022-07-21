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
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
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

    // since both source/target require establishing a connection, set the source as the target or
    // target as source if not set and connecting with bigtable
    String targetZkClusterConf = BigtableSyncTableAccessor.getTargetZkClusterConf(conf);
    String sourceZkClusterConf = BigtableSyncTableAccessor.getSourceZkClusterConf(conf);
    if (null == targetZkClusterConf && null != sourceZkClusterConf) {
      BigtableSyncTableAccessor.setTargetZkClusterConf(conf, sourceZkClusterConf);
      LOG.info(
          "target conn temporarily set as source for initialization only: " + sourceZkClusterConf);
    }

    if (null == sourceZkClusterConf && null != targetZkClusterConf) {
      BigtableSyncTableAccessor.setSourceZkClusterConf(conf, targetZkClusterConf);
      LOG.info(
          "source conn temporarily set as target for initialization only: " + targetZkClusterConf);
    }

    // SyncTable initializes map task configuration and connections with source and target. A valid
    // source and target are required for initialization. The connection is not used in the setup
    // and only required for initialization.
    super.setup(context);

    // establish target connection to bigtable
    if (!conf.onlyKeyExists(TARGET_BT_PROJECTID_CONF_KEY)) {
      createBigtableTargetConnection(conf);
    }

    // establish source connection to bigtable
    if (!conf.onlyKeyExists(SOURCE_BT_PROJECTID_CONF_KEY)) {
      createBigtableSourceConnection(conf);
    }
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
    Connection srcConn = BigtableSyncTableAccessor.setSourceConnection(this, bigtableConf);
    BigtableSyncTableAccessor.setSourceTable(this, srcConn, bigtableConf);
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
    Connection targetConn = BigtableSyncTableAccessor.setTargetConnection(this, bigtableConf);
    BigtableSyncTableAccessor.setTargetTable(this, targetConn, bigtableConf);
  }

  /**
   * close connection
   *
   * @param conn
   * @param sourceOrTarget
   */
  private void closeConnection(Connection conn, String sourceOrTarget) {
    try {
      if (conn != null) {
        conn.close();
      }
    } catch (IOException ioe) {
      LOG.debug("error closing temporary " + sourceOrTarget + " connection, " + ioe.getMessage());
      ioe.printStackTrace();
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

    bigtableConf.unset(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL);
    BigtableConfiguration.configure(
        bigtableConf,
        bigtableConf.get(projectIdKey),
        bigtableConf.get(instanceKey),
        bigtableConf.get(appProfileKey, ""));

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
