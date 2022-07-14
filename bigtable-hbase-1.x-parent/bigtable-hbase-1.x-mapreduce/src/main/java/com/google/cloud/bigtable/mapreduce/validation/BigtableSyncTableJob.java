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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.BigtableSyncTableAdapter;
import org.apache.hadoop.hbase.mapreduce.SyncTable;
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
 * functionality.
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
    if (!doCommandLine(otherArgs)) {
      return 1;
    }

    Job job =
        BigtableSyncTableAdapter.setupJobWithBigtable(
            this,
            otherArgs,
            sourceBigtableProjectId,
            sourceBigtableInstance,
            sourceBigtableAppProfile,
            targetBigtableProjectId,
            targetBigtableInstance,
            targetBigtableAppProfile);
    if (!job.waitForCompletion(true)) {
      LOG.info("Map-reduce job failed!");
      return 1;
    }
    counters = job.getCounters();
    return 0;
  }

  /**
   * SyncTable command line parsing utility
   *
   * @param args
   * @return
   */
  private boolean doCommandLine(final String[] args) {
    String[] toolDefaultArgs = parseBigtableCommandLineArgs(args);
    if (!BigtableSyncTableAdapter.parseCommandLine(this, NUM_ARGS, toolDefaultArgs)) return false;

    // return false if options and args are not properly set
    if (!BigtableSyncTableAdapter.verifyRequiredArgsSet(
        this,
        args,
        sourceBigtableProjectId,
        sourceBigtableInstance,
        targetBigtableProjectId,
        targetBigtableInstance)) return false;
    return true;
  }

  /**
   * parse bigtable configurations
   *
   * @param args
   * @return
   */
  public String[] parseBigtableCommandLineArgs(String[] args) {
    List<String> defaultArgs = new ArrayList<>();

    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

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

      String sourceBigtableAppProfileKey = "--sourcebigtableprofile=";
      if (cmd.startsWith(sourceBigtableAppProfileKey)) {
        sourceBigtableAppProfile = cmd.substring(sourceBigtableAppProfileKey.length());
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

      final String targetBigtableAppProfileKey = "--targetbigtableprofile=";
      if (cmd.startsWith(targetBigtableAppProfileKey)) {
        targetBigtableAppProfile = cmd.substring(targetBigtableAppProfileKey.length());
        continue;
      }

      defaultArgs.add(cmd);
    }

    return defaultArgs.toArray(new String[0]);
  }

  public static void printUsage(final String errorMsg, String[] args) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }

    if (args != null) {
      System.err.print("Input provided: ");
      for (int i = 0; i < args.length; i++) {
        if (i != 0) System.err.print(" ");
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
    System.err.println("    sourcebigtableprofile  (optional) Bigtable app profile");
    System.err.println(" Target Configuration:");
    System.err.println("    targetzkcluster  ZK cluster key of the target table");
    System.err.println("                      (defaults to cluster in classpath's config)");
    System.err.println(" Or Target Configuration:");
    System.err.println("    targetbigtableproject  Bigtable project id of the target table");
    System.err.println("                      (defaults to cluster in classpath's config)");
    System.err.println("    targetbigtableinstance  Bigtable instance id of the target table");
    System.err.println("                      (defaults to cluster in classpath's config)");
    System.err.println("    targetbigtableprofile  (optional) Bigtable app profile");
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
