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

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.BigtableSyncTableAdapter;
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

    // since both source/target require establishing a connection, mock the source as the target or
    // target as source if connecting with bigtable
    String targetZkClusterConf = BigtableSyncTableAdapter.getTargetZkClusterConf(conf);
    String sourceZkClusterConf = BigtableSyncTableAdapter.getSourceZkClusterConf(conf);
    if (null == targetZkClusterConf && null != sourceZkClusterConf) {
      BigtableSyncTableAdapter.setTargetZkClusterConf(conf, sourceZkClusterConf);
      LOG.debug("target conn init as source: " + sourceZkClusterConf);
    }

    if (null == sourceZkClusterConf && null != targetZkClusterConf) {
      BigtableSyncTableAdapter.setSourceZkClusterConf(conf, targetZkClusterConf);
      LOG.debug("source conn init as target: " + targetZkClusterConf);
    }

    // default setup
    super.setup(context);

    // establish target connection to bigtable
    if (!conf.onlyKeyExists(BigtableSyncTableJob.TARGET_BT_PROJECTID_CONF_KEY)) {
      BigtableSyncTableAdapter.createBigtableTargetConnection(this, conf);
    }

    // establish source connection to bigtable
    if (!conf.onlyKeyExists(BigtableSyncTableJob.SOURCE_BT_PROJECTID_CONF_KEY)) {
      BigtableSyncTableAdapter.createBigtableSourceConnection(this, conf);
    }
  }
}
