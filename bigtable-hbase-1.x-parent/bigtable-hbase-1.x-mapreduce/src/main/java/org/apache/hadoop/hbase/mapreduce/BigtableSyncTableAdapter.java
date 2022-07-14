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

import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.SOURCE_BT_APP_PROFILE_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.SOURCE_BT_INSTANCE_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.SOURCE_BT_PROJECTID_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.TARGET_BT_APP_PROFILE_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.TARGET_BT_INSTANCE_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.TARGET_BT_PROJECTID_CONF_KEY;
import static com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob.printUsage;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.mapreduce.validation.BigtableSyncMapper;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.SyncTable.SyncMapper;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.mapreduce.Job;

/** A helper class to access package private fields of SyncTable. */
public class BigtableSyncTableAdapter {

  private static final Log LOG = LogFactory.getLog(BigtableSyncTableAdapter.class);

  /**
   * Set up job configuration for Bigtable as Target
   *
   * @param syncTable
   * @param otherArgs
   * @param sourceBigtableProjectId
   * @param sourceBigtableInstance
   * @param sourceBigtableAppProfile
   * @param targetBigtableProjectId
   * @param targetBigtableInstance
   * @param targetBigtableAppProfile
   * @return
   * @throws IOException
   */
  public static Job setupJobWithBigtable(
      SyncTable syncTable,
      String[] otherArgs,
      String sourceBigtableProjectId,
      String sourceBigtableInstance,
      String sourceBigtableAppProfile,
      String targetBigtableProjectId,
      String targetBigtableInstance,
      String targetBigtableAppProfile)
      throws IOException {
    // set up default job configurations
    Job job = syncTable.createSubmittableJob(otherArgs);
    Configuration jobConf = job.getConfiguration();

    // set conf for job startup as InputFormat is initialized on target
    if (syncTable.targetZkCluster != null) {
      ZKConfig.ZKClusterKey zkClusterKey = ZKConfig.transformClusterKey(syncTable.targetZkCluster);
      jobConf.set(HConstants.ZOOKEEPER_QUORUM, zkClusterKey.getQuorumString());
      jobConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClusterKey.getClientPort());
      jobConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkClusterKey.getZnodeParent());
    }

    // enable dry run as default
    jobConf.setBoolean(
        SyncTable.DRY_RUN_CONF_KEY, jobConf.getBoolean(SyncTable.DRY_RUN_CONF_KEY, true));

    // Set up bigtable configurations for job. Note that the job conf is shared for source and
    // target databases and additional configuration in mapper initializes proper
    // connections.
    if (sourceBigtableProjectId != null && sourceBigtableInstance != null) {
      if (syncTable.sourceZkCluster != null)
        LOG.warn(
            "sourceZkCluster config("
                + syncTable.sourceZkCluster
                + ") overriden with sourceBigtableProjectId("
                + sourceBigtableProjectId
                + "), sourceBigtableInstance("
                + sourceBigtableInstance
                + ")");

      jobConf.set(SOURCE_BT_PROJECTID_CONF_KEY, sourceBigtableProjectId);
      jobConf.set(SOURCE_BT_INSTANCE_CONF_KEY, sourceBigtableInstance);

      if (sourceBigtableAppProfile != null)
        jobConf.set(SOURCE_BT_APP_PROFILE_CONF_KEY, sourceBigtableAppProfile);
    }
    if (targetBigtableProjectId != null && targetBigtableInstance != null) {
      if (syncTable.targetZkCluster != null)
        LOG.warn(
            "targetZkCluster config("
                + syncTable.targetZkCluster
                + ") overridden with targetBigtableProjectId("
                + targetBigtableProjectId
                + "), targetBigtableInstance("
                + targetBigtableInstance
                + ")");

      jobConf.set(TARGET_BT_PROJECTID_CONF_KEY, targetBigtableProjectId);
      jobConf.set(TARGET_BT_INSTANCE_CONF_KEY, targetBigtableInstance);

      if (targetBigtableAppProfile != null)
        jobConf.set(TARGET_BT_APP_PROFILE_CONF_KEY, targetBigtableAppProfile);

      BigtableConfiguration.configure(
          jobConf,
          jobConf.get(TARGET_BT_PROJECTID_CONF_KEY),
          jobConf.get(TARGET_BT_INSTANCE_CONF_KEY),
          jobConf.get(TARGET_BT_APP_PROFILE_CONF_KEY, ""));
    }

    Scan jobScan = TableMapReduceUtil.convertStringToScan(jobConf.get(TableInputFormat.SCAN));
    TableMapReduceUtil.initTableMapperJob(
        syncTable.targetTableName, jobScan, BigtableSyncMapper.class, null, null, job);

    return job;
  }

  /**
   * parse and set command line options and args
   *
   * @param syncTable
   * @param NUM_ARGS
   * @param args
   * @return
   */
  public static boolean parseCommandLine(SyncTable syncTable, int NUM_ARGS, final String[] args) {
    // set any defaults & override with any options/args that are set on cli
    setDefaultConfigs(syncTable);

    if (args.length < NUM_ARGS) {
      printUsage(null, args);
      return false;
    }
    try {
      syncTable.sourceHashDir = new Path(args[args.length - 3]);
      syncTable.sourceTableName = args[args.length - 2];
      syncTable.targetTableName = args[args.length - 1];

      for (int i = 0; i < args.length - NUM_ARGS; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null, args);
          return false;
        }

        final String sourceZkClusterKey = "--sourcezkcluster=";
        if (cmd.startsWith(sourceZkClusterKey)) {
          syncTable.sourceZkCluster = cmd.substring(sourceZkClusterKey.length());
          continue;
        }

        final String targetZkClusterKey = "--targetzkcluster=";
        if (cmd.startsWith(targetZkClusterKey)) {
          syncTable.targetZkCluster = cmd.substring(targetZkClusterKey.length());
          continue;
        }

        final String dryRunKey = "--dryrun=";
        if (cmd.startsWith(dryRunKey)) {
          syncTable.dryRun = Boolean.parseBoolean(cmd.substring(dryRunKey.length()));
          continue;
        }

        final String doDeletesKey = "--doDeletes=";
        if (cmd.startsWith(doDeletesKey)) {
          syncTable.doDeletes = Boolean.parseBoolean(cmd.substring(doDeletesKey.length()));
          continue;
        }

        final String doPutsKey = "--doPuts=";
        if (cmd.startsWith(doPutsKey)) {
          syncTable.doPuts = Boolean.parseBoolean(cmd.substring(doPutsKey.length()));
          continue;
        }

        final String ignoreTimestampsKey = "--ignoreTimestamps=";
        if (cmd.startsWith(ignoreTimestampsKey)) {
          syncTable.ignoreTimestamps =
              Boolean.parseBoolean(cmd.substring(ignoreTimestampsKey.length()));
          continue;
        }

        printUsage("Invalid argument '" + cmd + "'", args);
        return false;
      }

    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage(), args);
      return false;
    }
    return true;
  }

  /**
   * set default sync table configs explicitly
   *
   * @param syncTable
   */
  private static void setDefaultConfigs(SyncTable syncTable) {
    // dry run is enabled by default
    syncTable.dryRun = true;
  }

  /**
   * verify required options and args are correctly set
   *
   * @param syncTable
   * @param args
   * @param sourceBigtableProjectId
   * @param sourceBigtableInstance
   * @param targetBigtableProjectId
   * @param targetBigtableInstance
   * @return
   */
  public static boolean verifyRequiredArgsSet(
      SyncTable syncTable,
      String[] args,
      String sourceBigtableProjectId,
      String sourceBigtableInstance,
      String targetBigtableProjectId,
      String targetBigtableInstance) {
    if (syncTable.sourceZkCluster == null
        && (sourceBigtableProjectId == null || sourceBigtableInstance == null)) {
      printUsage(
          "--sourcezkcluster or --sourcebigtableproject and --sourcebigtableinstance required.",
          args);
      return false;
    }

    if (syncTable.targetZkCluster == null
        && (targetBigtableProjectId == null || targetBigtableInstance == null)) {
      printUsage(
          "--targetzkcluster or -targetbigtableproject and --targetbigtableinstance required.",
          args);
      return false;
    }

    if (syncTable.sourceHashDir == null) {
      printUsage("--sourcehashdir is required.", args);
      return false;
    }

    if (syncTable.sourceTableName == null) {
      printUsage("--sourcetablename is required.", args);
      return false;
    }

    if (syncTable.targetTableName == null) {
      printUsage("--targettablename is required.", args);
      return false;
    }

    return true;
  }

  /**
   * create source connection with Bigtable
   *
   * @param mapper
   * @param conf
   * @throws IOException
   */
  public static void createBigtableSourceConnection(SyncMapper mapper, Configuration conf)
      throws IOException {
    // create source connection config
    // inherit base config, but override connection based on job args
    Configuration bigtableConf = new Configuration(conf);
    bigtableConf.unset(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL);
    BigtableConfiguration.configure(
        bigtableConf,
        bigtableConf.get(SOURCE_BT_PROJECTID_CONF_KEY),
        bigtableConf.get(SOURCE_BT_INSTANCE_CONF_KEY),
        bigtableConf.get(SOURCE_BT_APP_PROFILE_CONF_KEY, ""));

    LOG.info(
        "configuring source connection to Bigtable with configurations: "
            + bigtableConf.get(SOURCE_BT_PROJECTID_CONF_KEY)
            + ", "
            + bigtableConf.get(SOURCE_BT_INSTANCE_CONF_KEY)
            + ", "
            + bigtableConf.get(SOURCE_BT_APP_PROFILE_CONF_KEY, ""));

    try {
      if (mapper.sourceConnection != null) {
        mapper.sourceConnection.close();
      }
    } catch (IOException ioe) {
      LOG.warn("closed source connection, " + ioe.getMessage());
      ioe.printStackTrace();
    }

    mapper.sourceConnection = openConnection(bigtableConf);
    mapper.sourceTable =
        openTable(mapper.sourceConnection, bigtableConf, SyncTable.SOURCE_TABLE_CONF_KEY);
  }

  /**
   * create target connection with Bigtable
   *
   * @param mapper
   * @param conf
   * @throws IOException
   */
  public static void createBigtableTargetConnection(SyncMapper mapper, Configuration conf)
      throws IOException {
    // create target connection config
    // inherit base config, but override connection based on job args
    Configuration bigtableConf = new Configuration(conf);
    bigtableConf.unset(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL);
    BigtableConfiguration.configure(
        bigtableConf,
        bigtableConf.get(TARGET_BT_PROJECTID_CONF_KEY),
        bigtableConf.get(TARGET_BT_INSTANCE_CONF_KEY),
        bigtableConf.get(TARGET_BT_APP_PROFILE_CONF_KEY, ""));

    LOG.info(
        "configuring target connection to Bigtable with configurations: "
            + bigtableConf.get(TARGET_BT_PROJECTID_CONF_KEY)
            + ", "
            + bigtableConf.get(TARGET_BT_INSTANCE_CONF_KEY)
            + ", "
            + bigtableConf.get(TARGET_BT_APP_PROFILE_CONF_KEY, ""));
    try {
      if (mapper.targetConnection != null) {
        mapper.targetConnection.close();
      }
    } catch (IOException ioe) {
      LOG.warn("closed target connection, " + ioe.getMessage());
      ioe.printStackTrace();
    }

    mapper.targetConnection = openConnection(bigtableConf);
    mapper.targetTable =
        openTable(mapper.targetConnection, bigtableConf, SyncTable.TARGET_TABLE_CONF_KEY);
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

  private static Connection openConnection(Configuration conf) throws IOException {
    return ConnectionFactory.createConnection(conf);
  }

  private static Table openTable(Connection connection, Configuration conf, String tableNameConfKey)
      throws IOException {
    return connection.getTable(TableName.valueOf(conf.get(tableNameConfKey)));
  }
}
