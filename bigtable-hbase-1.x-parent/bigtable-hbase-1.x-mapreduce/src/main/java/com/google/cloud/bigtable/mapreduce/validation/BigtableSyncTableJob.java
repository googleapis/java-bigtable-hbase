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

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.BigtableSyncTableAccessor;
import org.apache.hadoop.hbase.mapreduce.SyncTable;
import org.apache.hadoop.hbase.mapreduce.SyncTable.SyncMapper.Counter;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

/**
 * Bigtable SyncTable Job
 *
 * <p>Base source authored by {@link org.apache.hadoop.hbase.mapreduce.SyncTable}.
 *
 * <p>SyncTable is part 2 of 2 stages used for byte level validation between a source and target
 * table only. BigtableSyncTableJob extends functionality to provide connectivity to Bigtable as a
 * source or target and relies on {@link org.apache.hadoop.hbase.mapreduce.SyncTable} for core
 * functionality. BigtableSyncTableJob overrides dryrun configuration as true by default to perform
 * a read-only validation report only.
 *
 * <p>SyncTable is a Mapreduce job that generates hashes on the target table and compares these
 * hashes with the output from {@Link org.apache.hadoop.hbase.mapreduce.HashTable}. For diverging
 * hashes, cell-level comparison is performed.
 */
public class BigtableSyncTableJob extends SyncTable {

  private static final Log LOG = LogFactory.getLog(BigtableSyncTableJob.class);

  public static final String SOURCE_BT_PROJECTID_CONF_KEY = "sync.table.source.bt.projectid";
  public static final String SOURCE_BT_INSTANCE_CONF_KEY = "sync.table.source.bt.instance";
  public static final String SOURCE_BT_APP_PROFILE_CONF_KEY = "sync.table.source.bt.app.profile";
  public static final String TARGET_BT_PROJECTID_CONF_KEY = "sync.table.target.bt.projectid";
  public static final String TARGET_BT_INSTANCE_CONF_KEY = "sync.table.target.bt.instance";
  public static final String TARGET_BT_APP_PROFILE_CONF_KEY = "sync.table.target.bt.app.profile";

  private String sourceBigtableProjectId;
  private String sourceBigtableInstance;
  private String sourceBigtableAppProfile;

  private String targetBigtableProjectId;
  private String targetBigtableInstance;
  private String targetBigtableAppProfile;

  private static final int NUM_ARGS = 3;

  protected Counters counters;

  public BigtableSyncTableJob(Configuration conf) {
    super(conf);
  }

  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    if (!doCommandLine(this, otherArgs)) {
      return 1;
    }

    Job job = setupJobWithBigtable(otherArgs);
    if (!job.waitForCompletion(true)) {
      LOG.info("Map-reduce job failed!");
      return 1;
    }
    counters = job.getCounters();
    long batches = counters.findCounter(Counter.BATCHES).getValue();
    long hashesMatched = counters.findCounter(Counter.HASHES_MATCHED).getValue();
    long hashesNotMatched = counters.findCounter(Counter.HASHES_NOT_MATCHED).getValue();

    LOG.info("############# Num of validation batches = " + batches);
    LOG.info("############# Num of validation hashes matched = " + hashesMatched);
    LOG.info("############# Num of validation hashes not matched = " + hashesNotMatched);

    return 0;
  }

  /**
   * SyncTable command line parsing utility
   *
   * @param args
   * @return
   */
  protected boolean doCommandLine(SyncTable syncTable, final String[] args) {
    if (!parseCommandLine(syncTable, NUM_ARGS, args)) {
      return false;
    }
    // return false if options and args are not properly set
    if (!verifyRequiredArgsSet(
        syncTable,
        args,
        sourceBigtableProjectId,
        sourceBigtableInstance,
        targetBigtableProjectId,
        targetBigtableInstance)) {
      return false;
    }
    return true;
  }

  /**
   * set default sync table configs explicitly
   *
   * @param syncTable
   */
  private void setDefaultConfigs(SyncTable syncTable) {
    // dry run is enabled by default
    BigtableSyncTableAccessor.setDryRun(syncTable, true);
  }

  /**
   * parse and set command line options and args
   *
   * @param syncTable
   * @param NUM_ARGS
   * @param args
   * @return
   */
  private boolean parseCommandLine(SyncTable syncTable, int NUM_ARGS, final String[] args) {
    // set any defaults & override with any options/args that are set on cli
    setDefaultConfigs(syncTable);

    if (args.length < NUM_ARGS) {
      printUsage(null, args);
      return false;
    }
    try {
      BigtableSyncTableAccessor.setSourceHashDir(syncTable, new Path(args[args.length - 3]));
      BigtableSyncTableAccessor.setSourceTableName(syncTable, args[args.length - 2]);
      BigtableSyncTableAccessor.setTargetTableName(syncTable, args[args.length - 1]);

      int cntArgs = 0;
      for (int i = 0; i < args.length; i++) {
        String arg = args[i];
        if (arg.equals("-h") || arg.startsWith("--h")) {
          printUsage(null, args);
          return false;
        }

        if (!arg.startsWith("--")) {
          cntArgs++;
        }
      }

      if (cntArgs != 3) {
        printUsage("expected " + NUM_ARGS + ", but received " + cntArgs++, args);
        return false;
      }

      for (int i = 0; i < args.length - NUM_ARGS; i++) {
        String cmd = args[i];

        final String sourceZkClusterKey = "--sourcezkcluster=";
        if (cmd.startsWith(sourceZkClusterKey)) {
          BigtableSyncTableAccessor.setSourceZkCluster(
              syncTable, cmd.substring(sourceZkClusterKey.length()));
          continue;
        }

        String sourceBigtableProjectIdKey = "--sourcebigtableproject=";
        if (cmd.startsWith(sourceBigtableProjectIdKey)) {
          sourceBigtableProjectId = cmd.substring(sourceBigtableProjectIdKey.length());
          continue;
        }

        String sourceBigtableInstanceKey = "--sourcebigtableinstance=";
        if (cmd.startsWith(sourceBigtableInstanceKey)) {
          sourceBigtableInstance = cmd.substring(sourceBigtableInstanceKey.length());
          continue;
        }

        String sourceBigtableAppProfileKey = "--sourcebigtableappprofile=";
        if (cmd.startsWith(sourceBigtableAppProfileKey)) {
          sourceBigtableAppProfile = cmd.substring(sourceBigtableAppProfileKey.length());
          continue;
        }

        final String targetZkClusterKey = "--targetzkcluster=";
        if (cmd.startsWith(targetZkClusterKey)) {
          BigtableSyncTableAccessor.setTargetZkCluster(
              syncTable, cmd.substring(targetZkClusterKey.length()));
          continue;
        }

        final String targetBigtableProjectIdKey = "--targetbigtableproject=";
        if (cmd.startsWith(targetBigtableProjectIdKey)) {
          targetBigtableProjectId = cmd.substring(targetBigtableProjectIdKey.length());
          continue;
        }

        final String targetBigtableInstanceKey = "--targetbigtableinstance=";
        if (cmd.startsWith(targetBigtableInstanceKey)) {
          targetBigtableInstance = cmd.substring(targetBigtableInstanceKey.length());
          continue;
        }

        final String targetBigtableAppProfileKey = "--targetbigtableappprofile=";
        if (cmd.startsWith(targetBigtableAppProfileKey)) {
          targetBigtableAppProfile = cmd.substring(targetBigtableAppProfileKey.length());
          continue;
        }

        final String dryRunKey = "--dryrun=";
        if (cmd.startsWith(dryRunKey)) {
          BigtableSyncTableAccessor.setDryRun(
              syncTable, Boolean.parseBoolean(cmd.substring(dryRunKey.length())));
          continue;
        }

        final String doDeletesKey = "--doDeletes=";
        if (cmd.startsWith(doDeletesKey)) {
          BigtableSyncTableAccessor.setDoDeletes(
              syncTable, Boolean.parseBoolean(cmd.substring(doDeletesKey.length())));
          continue;
        }

        final String doPutsKey = "--doPuts=";
        if (cmd.startsWith(doPutsKey)) {
          BigtableSyncTableAccessor.setDoPuts(
              syncTable, Boolean.parseBoolean(cmd.substring(doPutsKey.length())));
          continue;
        }

        final String ignoreTimestampsKey = "--ignoreTimestamps=";
        if (cmd.startsWith(ignoreTimestampsKey)) {
          BigtableSyncTableAccessor.setIgnoreTimestamps(
              syncTable, Boolean.parseBoolean(cmd.substring(ignoreTimestampsKey.length())));
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
   * verify required options and args are correctly set
   *
   * @param args
   * @param sourceBigtableProjectId
   * @param sourceBigtableInstance
   * @param targetBigtableProjectId
   * @param targetBigtableInstance
   * @return boolean state if required args are set
   */
  public boolean verifyRequiredArgsSet(
      SyncTable syncTable,
      String[] args,
      String sourceBigtableProjectId,
      String sourceBigtableInstance,
      String targetBigtableProjectId,
      String targetBigtableInstance) {

    // neither source zk or source bigtable config set
    if (BigtableSyncTableAccessor.getSourceZkCluster(syncTable) == null
        && (sourceBigtableProjectId == null && sourceBigtableInstance == null)) {
      printUsage(
          "--sourcezkcluster or --sourcebigtableproject and --sourcebigtableinstance required.",
          args);
      return false;
    }

    // both source zk and source bigtable configs are set
    if (BigtableSyncTableAccessor.getSourceZkCluster(syncTable) != null
        && (sourceBigtableProjectId != null || sourceBigtableInstance != null)) {
      printUsage(
          "--sourcezkcluster and --sourcebigtableproject and --sourcebigtableinstance are set. Set one or the other.",
          args);
      return false;
    }

    // source bigtable project or source instance not set
    if (BigtableSyncTableAccessor.getSourceZkCluster(syncTable) == null
        && ((sourceBigtableProjectId == null || sourceBigtableInstance == null))) {
      printUsage("--sourcebigtableproject and --sourcebigtableinstance required.", args);
      return false;
    }

    // neither target zk or target bigtable config set
    if (BigtableSyncTableAccessor.getTargetZkCluster(syncTable) == null
        && (targetBigtableProjectId == null && targetBigtableInstance == null)) {
      printUsage(
          "--targetzkcluster or --targetbigtableproject and --targetbigtableinstance required.",
          args);
      return false;
    }

    // both target zk and target bigtable configs are set
    if (BigtableSyncTableAccessor.getTargetZkCluster(syncTable) != null
        && (targetBigtableProjectId != null || targetBigtableInstance != null)) {
      printUsage(
          "--targetzkcluster or --targetbigtableproject and --targetbigtableinstance are set. Set one or the other.",
          args);
      return false;
    }

    // target bigtable project or target instance not set
    if (BigtableSyncTableAccessor.getTargetZkCluster(syncTable) == null
        && ((targetBigtableProjectId == null || targetBigtableInstance == null))) {
      printUsage("--targetbigtableproject and --targetbigtableinstance required.", args);
      return false;
    }

    if (BigtableSyncTableAccessor.getSourceHashDir(syncTable) == null) {
      printUsage("<sourcehashdir> argument is required.", args);
      return false;
    }

    if (BigtableSyncTableAccessor.getSourceTableName(syncTable) == null) {
      printUsage("<sourcetable> argument is required.", args);
      return false;
    }

    if (BigtableSyncTableAccessor.getTargetTableName(syncTable) == null) {
      printUsage("<targettable> argument is required.", args);
      return false;
    }

    return true;
  }

  public static void printUsage(final String errorMsg, String[] args) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }

    if (args != null) {
      System.err.print("Input provided: ");
      for (int i = 0; i < args.length; i++) {
        if (i != 0) {
          System.err.print(" ");
        }
        System.err.print(args[i]);
      }
      System.err.println();
    }

    System.err.println("Usage: SyncTable [options] <sourcehashdir> <sourcetable> <targettable>");
    System.err.println();
    System.err.println("Options:");

    System.err.println(" Source Configuration:");
    System.err.println("    sourcezkcluster   ZK cluster key of the source table");
    System.err.println("                      (defaults to cluster in classpath's config)");
    System.err.println(" Or Source Configuration:");
    System.err.println("    sourcebigtableproject  Bigtable project id of the source table");
    System.err.println("                      (defaults to cluster in classpath's config)");
    System.err.println("    sourcebigtableinstance  Bigtable instance id of the source table");
    System.err.println("                      (defaults to cluster in classpath's config)");
    System.err.println("    sourcebigtableappprofile  (optional) Bigtable app profile");
    System.err.println(" Target Configuration:");
    System.err.println("    targetzkcluster  ZK cluster key of the target table");
    System.err.println("                      (defaults to cluster in classpath's config)");
    System.err.println(" Or Target Configuration:");
    System.err.println("    targetbigtableproject  Bigtable project id of the target table");
    System.err.println("                      (defaults to cluster in classpath's config)");
    System.err.println("    targetbigtableinstance  Bigtable instance id of the target table");
    System.err.println("                      (defaults to cluster in classpath's config)");
    System.err.println("    targetbigtableappprofile  (optional) Bigtable app profile");
    System.err.println();
    System.err.println(" dryrun           if true, output counters but no writes");
    System.err.println("                  (defaults to true)");
    System.err.println(" doDeletes        if false, does not perform deletes");
    System.err.println("                  (defaults to true)");
    System.err.println(" doPuts           if false, does not perform puts");
    System.err.println("                  (defaults to true)");
    System.err.println(" ignoreTimestamps if true, ignores cells timestamps while comparing ");
    System.err.println("                  cell values. Any missing cell on target then gets");
    System.err.println("                  added with current time as timestamp ");
    System.err.println("                  (defaults to false)");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" sourcehashdir    path to HashTable output dir for source table");
    System.err.println("                  (see org.apache.hadoop.hbase.mapreduce.HashTable)");
    System.err.println(" sourcetable      Name of the source table to sync from");
    System.err.println(" targettable      Name of the target table to sync to");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" For SyncTable validation of tableA from a remote source cluster");
    System.err.println(" to a local target cluster:");
    System.err.println(
        " $ bin/hbase "
            + "com.google.cloud.bigtable.mapreduce.validation.SyncTable --targetbigtableproject=project123"
            + " --targetbigtableinstance=instance123 --sourcezkcluster=zk1.example.com,zk2.example.com,zk3.example.com:2181:/hbase"
            + " gs://bucket/hashes/tableA tableA tableA");
  }

  /**
   * Set up job configuration for Bigtable as Target
   *
   * @param otherArgs
   * @return
   * @throws IOException
   */
  private Job setupJobWithBigtable(String[] otherArgs) throws IOException {
    // set up default job configurations
    Job job = super.createSubmittableJob(otherArgs);
    Configuration jobConf = job.getConfiguration();

    // set conf for job startup as InputFormat is initialized on target
    String targetZkCluster = BigtableSyncTableAccessor.getTargetZkCluster(this);
    if (targetZkCluster != null) {
      ZKConfig.ZKClusterKey zkClusterKey = ZKConfig.transformClusterKey(targetZkCluster);
      jobConf.set(HConstants.ZOOKEEPER_QUORUM, zkClusterKey.getQuorumString());
      jobConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zkClusterKey.getClientPort());
      jobConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkClusterKey.getZnodeParent());
    }

    // set bigtable target configuration
    if (targetBigtableProjectId != null && targetBigtableInstance != null) {
      if (targetZkCluster != null) {
        LOG.warn(
            "targetZkCluster config("
                + targetZkCluster
                + ") overridden with targetBigtableProjectId("
                + targetBigtableProjectId
                + "), targetBigtableInstance("
                + targetBigtableInstance
                + ")");
      }

      jobConf.set(TARGET_BT_PROJECTID_CONF_KEY, targetBigtableProjectId);
      jobConf.set(TARGET_BT_INSTANCE_CONF_KEY, targetBigtableInstance);

      if (targetBigtableAppProfile != null) {
        jobConf.set(TARGET_BT_APP_PROFILE_CONF_KEY, targetBigtableAppProfile);
      }

      BigtableConfiguration.configure(
          jobConf,
          jobConf.get(TARGET_BT_PROJECTID_CONF_KEY),
          jobConf.get(TARGET_BT_INSTANCE_CONF_KEY),
          jobConf.get(TARGET_BT_APP_PROFILE_CONF_KEY, ""));
    }

    // enable dry run as default
    String DRY_RUN_CONF_KEY = BigtableSyncTableAccessor.getConfDryRunKey();
    jobConf.setBoolean(DRY_RUN_CONF_KEY, jobConf.getBoolean(DRY_RUN_CONF_KEY, true));

    // Set up bigtable configurations for job. Note that the job conf is shared for source and
    // target databases and additional configuration in mapper initializes proper
    // connections.
    String sourceZkCluster = BigtableSyncTableAccessor.getSourceZkCluster(this);
    if (sourceBigtableProjectId != null && sourceBigtableInstance != null) {
      if (sourceZkCluster != null) {
        LOG.warn(
            "sourceZkCluster config("
                + sourceZkCluster
                + ") overriden with sourceBigtableProjectId("
                + sourceBigtableProjectId
                + "), sourceBigtableInstance("
                + sourceBigtableInstance
                + ")");
      }

      jobConf.set(SOURCE_BT_PROJECTID_CONF_KEY, sourceBigtableProjectId);
      jobConf.set(SOURCE_BT_INSTANCE_CONF_KEY, sourceBigtableInstance);

      if (sourceBigtableAppProfile != null) {
        jobConf.set(SOURCE_BT_APP_PROFILE_CONF_KEY, sourceBigtableAppProfile);
      }
    }

    Scan jobScan = TableMapReduceUtil.convertStringToScan(jobConf.get(TableInputFormat.SCAN));
    TableMapReduceUtil.initTableMapperJob(
        BigtableSyncTableAccessor.getTargetTableName(this),
        jobScan,
        BigtableSyncMapper.class,
        null,
        null,
        job);

    return job;
  }

  public String getSourceBigtableProjectId() {
    return sourceBigtableProjectId;
  }

  public String getSourceBigtableInstance() {
    return sourceBigtableInstance;
  }

  public String getSourceBigtableAppProfile() {
    return sourceBigtableAppProfile;
  }

  public String getTargetBigtableProjectId() {
    return targetBigtableProjectId;
  }

  public String getTargetBigtableInstance() {
    return targetBigtableInstance;
  }

  public String getTargetBigtableAppProfile() {
    return targetBigtableAppProfile;
  }

  /** Main entry point. */
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new BigtableSyncTableJob(HBaseConfiguration.create()), args);

    if (exitCode == 0) {
      System.out.println("job appears to have completed successfully.");
    } else {
      System.err.println("job is exiting with exit code='" + exitCode + "'");
    }
    System.exit(exitCode);
  }
}
