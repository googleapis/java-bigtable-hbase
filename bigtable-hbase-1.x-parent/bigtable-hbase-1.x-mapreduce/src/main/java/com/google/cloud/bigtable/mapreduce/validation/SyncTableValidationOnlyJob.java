/*
 * Copyright 2021 Google LLC
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
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.BigtableTableHashAccessor;
import org.apache.hadoop.hbase.mapreduce.BigtableTableHashAccessor.BigtableResultHasher;
import org.apache.hadoop.hbase.mapreduce.HashTable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Base source authored by {@link org.apache.hadoop.hbase.mapreduce.SyncTable}.
 *
 * <p>SyncTable is part 2 of 2 stages used for byte level validation between a source and target
 * table only. SyncTableValidationOnlyJob is solely for validation and does not support
 * synchronizing differences from source to target.
 *
 * <p>SyncTable is a Mapreduce job that generates hashes on the target table and compares these
 * hashes with the output from {@Link org.apache.hadoop.hbase.mapreduce.HashTable}. For diverging
 * hashes, cell-level comparison is performed and mismatch results outputted to a filesystem (or
 * cloud storage).
 *
 * <p>The tool may operate without connectivity to source, but won't perform cell-level comparison
 * and emit cell metadata with reason code. With source connectivity, below sample output will be
 * emitted upon mismatch identified.
 *
 * <p>{"reason_code":"TARGET_MISSING_CELLS","cell":{"row":"row_key_1","family":"cf1","column":"col1","timestamp":40,"vlen":8,"type":"Put"}}
 * {"reason_code":"DIFFERENT_CELL_VALUES","source_cell":{"row":"row_key_1","family":"cf1","column":"col1","timestamp":5,"vlen":14,"type":"Put"},"target_cell":{"row":"row_key_1","family":"cf1","column":"col1","timestamp":5,"vlen":13,"type":"Put"}}
 * {"reason_code":"SOURCE_MISSING_CELLS","cell":{"row":"row_key_1","family":"cf1","column":"col1","timestamp":4,"vlen":8,"type":"Put"}}
 */
public class SyncTableValidationOnlyJob extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(SyncTableValidationOnlyJob.class);

  private static final String SOURCE_HASH_DIR_CONF_KEY = "sync.table.source.hash.dir";
  private static final String SOURCE_TABLE_CONF_KEY = "sync.table.source.table.name";
  private static final String TARGET_TABLE_CONF_KEY = "sync.table.target.table.name";
  private static final String SOURCE_ZK_CLUSTER_CONF_KEY = "sync.table.source.zk.cluster";
  private static final String SOURCE_BT_PROJECTID_CONF_KEY = "sync.table.source.bt.projectid";
  private static final String SOURCE_BT_INSTANCE_CONF_KEY = "sync.table.source.bt.instance";
  private static final String SOURCE_BT_APP_PROFILE_CONF_KEY = "sync.table.source.bt.app.profile";
  private static final String TARGET_ZK_CLUSTER_CONF_KEY = "sync.table.target.zk.cluster";
  private static final String TARGET_BT_PROJECTID_CONF_KEY = "sync.table.target.bt.projectid";
  private static final String TARGET_BT_INSTANCE_CONF_KEY = "sync.table.target.bt.instance";
  private static final String TARGET_BT_APP_PROFILE_CONF_KEY = "sync.table.target.bt.app.profile";

  private Path sourceHashDir;
  private String sourceTableName;
  private String targetTableName;
  private String syncOutputDir;

  private String sourceZkCluster;
  private String sourceBigtableProjectId;
  private String sourceBigtableInstance;
  private String sourceBigtableAppProfile;
  private String targetZkCluster;
  private String targetBigtableProjectId;
  private String targetBigtableInstance;
  private String targetBigtableAppProfile;

  protected Counters counters;

  public SyncTableValidationOnlyJob(Configuration conf) {
    super(conf);
  }

  private static void printUsage(String errorMsg, String[] args) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }

    if (args != null) {
      System.err.print("Input provided: ");
      for (int i = 0; i < args.length; i++) {
        if (i != 0) System.err.print(" ");
        System.err.print(args[i]);
      }
    }

    System.err.println("Usage: SyncTable [options]");
    System.err.println();
    System.err.println("Required Options:");
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
    System.err.println(" sourcehashdir     path to HashTable output dir for source table");
    System.err.println("                   (see org.apache.hadoop.hbase.mapreduce.HashTable)");
    System.err.println(" sourcetablename   Name of the source table to sync from");
    System.err.println(" targettablename   Name of the target table to sync to");
    System.err.println(" outputpath        Filesystem path to put the sync output data");
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

  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    if (!doCommandLine(otherArgs)) {
      return 1;
    }

    Job job = createSubmittableJob(otherArgs);
    if (!job.waitForCompletion(true)) {
      return 1;
    }
    counters = job.getCounters();
    return 0;
  }

  protected boolean doCommandLine(String[] args) {
    if (!parseCommandLine(args)) return false;
    if (!verifyRequiredArgs(args)) return false;
    return true;
  }

  private boolean verifyRequiredArgs(String[] args) {
    if (sourceZkCluster == null
        && (sourceBigtableProjectId == null || sourceBigtableInstance == null)) {
      printUsage(
          "--sourcezkcluster or --sourcebigtableproject and --sourcebigtableinstance required.",
          args);
      return false;
    }

    if (sourceHashDir == null) {
      printUsage("--sourcehashdir is required.", args);
      return false;
    }

    if (sourceTableName == null) {
      printUsage("--sourcetablename is required.", args);
      return false;
    }

    if (targetTableName == null) {
      printUsage("--targettablename is required.", args);
      return false;
    }

    if (syncOutputDir == null) {
      printUsage("--outputpath is required.", args);
      return false;
    }

    return true;
  }

  private boolean parseCommandLine(String[] args) {
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null, args);
          return false;
        }

        // required
        String sourceZkClusterKey = "--sourcezkcluster=";
        if (cmd.startsWith(sourceZkClusterKey)) {
          sourceZkCluster = cmd.substring(sourceZkClusterKey.length());
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

        String sourceBigtableAppProfileKey = "--sourcebigtableprofile=";
        if (cmd.startsWith(sourceBigtableAppProfileKey)) {
          sourceBigtableAppProfile = cmd.substring(sourceBigtableAppProfileKey.length());
          continue;
        }

        String targetZkClusterKey = "--targetzkcluster=";
        if (cmd.startsWith(targetZkClusterKey)) {
          targetZkCluster = cmd.substring(targetZkClusterKey.length());
          continue;
        }

        String targetBigtableProjectIdKey = "--targetbigtableproject=";
        if (cmd.startsWith(targetBigtableProjectIdKey)) {
          targetBigtableProjectId = cmd.substring(targetBigtableProjectIdKey.length());
          continue;
        }

        String targetBigtableInstanceKey = "--targetbigtableinstance=";
        if (cmd.startsWith(targetBigtableInstanceKey)) {
          targetBigtableInstance = cmd.substring(targetBigtableInstanceKey.length());
          continue;
        }

        String targetBigtableAppProfileKey = "--targetbigtableprofile=";
        if (cmd.startsWith(targetBigtableAppProfileKey)) {
          targetBigtableAppProfile = cmd.substring(targetBigtableAppProfileKey.length());
          continue;
        }

        String sourceHashDirKey = "--sourcehashdir=";
        if (cmd.startsWith(sourceHashDirKey)) {
          sourceHashDir = new Path(cmd.substring(sourceHashDirKey.length()));
          continue;
        }

        String sourceTableNameKey = "--sourcetablename=";
        if (cmd.startsWith(sourceTableNameKey)) {
          sourceTableName = cmd.substring(sourceTableNameKey.length());
          continue;
        }

        String targetTableNameKey = "--targettablename=";
        if (cmd.startsWith(targetTableNameKey)) {
          targetTableName = cmd.substring(targetTableNameKey.length());
          continue;
        }

        String syncOutputDirKey = "--outputpath=";
        if (cmd.startsWith(syncOutputDirKey)) {
          syncOutputDir = cmd.substring(syncOutputDirKey.length());
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

  private Configuration initCredentialsForHBase(String zookeeper, Job job) throws IOException {
    Configuration peerConf =
        HBaseConfiguration.createClusterConf(job.getConfiguration(), zookeeper);

    if ("kerberos".equalsIgnoreCase(peerConf.get("hbase.security.authentication"))) {
      TableMapReduceUtil.initCredentialsForCluster(job, peerConf);
    }

    return peerConf;
  }

  public Job createSubmittableJob(String[] args) throws IOException {
    FileSystem fs = sourceHashDir.getFileSystem(getConf());
    if (!fs.exists(sourceHashDir)) {
      throw new IOException("Source hash dir not found: " + sourceHashDir);
    }

    Job job =
        Job.getInstance(
            getConf(),
            getConf()
                .get(MRJobConfig.JOB_NAME, "syncTable_" + sourceTableName + "-" + targetTableName));
    Configuration jobConf = job.getConfiguration();
    if ("kerberos".equalsIgnoreCase(jobConf.get("hadoop.security.authentication"))) {
      TokenCache.obtainTokensForNamenodes(
          job.getCredentials(), new Path[] {sourceHashDir}, getConf());
    }

    HashTable.TableHash tableHash = HashTable.TableHash.read(getConf(), sourceHashDir);
    String tableName = BigtableTableHashAccessor.getTableName(tableHash);
    int numHashFiles = BigtableTableHashAccessor.getNumHashFiles(tableHash);
    String hashDataDir = BigtableTableHashAccessor.getHashDataDir();
    String hashOutputDataFilePrefix = BigtableTableHashAccessor.getHashOutputDataFilePrefix();
    List<ImmutableBytesWritable> partitions = BigtableTableHashAccessor.getPartitions(tableHash);

    LOG.info("Read source hash manifest: " + tableHash);
    LOG.info("Read " + partitions.size() + " partition keys");
    if (!tableName.equals(sourceTableName)) {
      LOG.warn(
          "Table name mismatch - manifest indicates hash was taken from: "
              + tableName
              + " but job is reading from: "
              + sourceTableName);
    }
    if (numHashFiles != partitions.size() + 1) {
      throw new RuntimeException(
          "Hash data appears corrupt. The number of of hash files created"
              + " should be 1 more than the number of partition keys.  However, the manifest file "
              + " says numHashFiles="
              + numHashFiles
              + " but the number of partition keys"
              + " found in the partitions file is "
              + partitions.size());
    }

    Path dataDir = new Path(sourceHashDir, hashDataDir);
    int dataSubdirCount = 0;
    for (FileStatus file : fs.listStatus(dataDir)) {
      if (file.getPath().getName().startsWith(hashOutputDataFilePrefix)) {
        dataSubdirCount++;
      }
    }

    if (dataSubdirCount != numHashFiles) {
      throw new RuntimeException(
          "Hash data appears corrupt. The number of of hash files created"
              + " should be 1 more than the number of partition keys.  However, the number of data dirs"
              + " found is "
              + dataSubdirCount
              + " but the number of partition keys"
              + " found in the partitions file is "
              + partitions.size());
    }

    job.setJarByClass(HashTable.class);
    jobConf.set(SOURCE_HASH_DIR_CONF_KEY, sourceHashDir.toString());
    jobConf.set(SOURCE_TABLE_CONF_KEY, sourceTableName);
    jobConf.set(TARGET_TABLE_CONF_KEY, targetTableName);
    if (sourceZkCluster != null) {
      jobConf.set(SOURCE_ZK_CLUSTER_CONF_KEY, sourceZkCluster);
      initCredentialsForHBase(sourceZkCluster, job);
    }
    if (sourceBigtableProjectId != null && sourceBigtableInstance != null) {
      if (sourceZkCluster != null)
        LOG.warn(
            "sourceZkCluster config("
                + sourceZkCluster
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
    // override cluster config
    if (targetZkCluster != null) {
      jobConf.set(TARGET_ZK_CLUSTER_CONF_KEY, targetZkCluster);
      Configuration peerConf = initCredentialsForHBase(targetZkCluster, job);

      // set conf for job startup as InputFormat is initialized on target
      jobConf.set(HConstants.ZOOKEEPER_QUORUM, peerConf.get(HConstants.ZOOKEEPER_QUORUM));
      jobConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, peerConf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
      jobConf.set(
          HConstants.ZOOKEEPER_ZNODE_PARENT, peerConf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    }
    // override cluster config
    if (targetBigtableProjectId != null && targetBigtableInstance != null) {
      if (targetZkCluster != null)
        LOG.warn(
            "targetZkCluster config("
                + targetZkCluster
                + ") overridden with targetBigtableProjectId("
                + targetBigtableProjectId
                + "), targetBigtableInstance("
                + targetBigtableInstance
                + ")");
      jobConf.set(TARGET_BT_PROJECTID_CONF_KEY, targetBigtableProjectId);
      jobConf.set(TARGET_BT_INSTANCE_CONF_KEY, targetBigtableInstance);

      if (targetBigtableAppProfile != null)
        jobConf.set(TARGET_BT_APP_PROFILE_CONF_KEY, targetBigtableAppProfile);

      // set conf for job startup as InputFormat is initialized on target
      BigtableConfiguration.configure(
          jobConf,
          jobConf.get(TARGET_BT_PROJECTID_CONF_KEY),
          jobConf.get(TARGET_BT_INSTANCE_CONF_KEY),
          jobConf.get(TARGET_BT_APP_PROFILE_CONF_KEY, ""));
    }

    TableMapReduceUtil.initTableMapperJob(
        targetTableName,
        BigtableTableHashAccessor.getScan(tableHash),
        SyncMapper.class,
        null,
        null,
        job);

    FileOutputFormat.setOutputPath(job, new Path(syncOutputDir));
    job.setNumReduceTasks(0);

    return job;
  }

  /** MAPPER */
  public static class SyncMapper extends TableMapper<NullWritable, Text> {
    private Path sourceHashDir;

    private Connection sourceConnection;
    private Connection targetConnection;
    private Table sourceTable;
    private Table targetTable;

    private HashTable.TableHash sourceTableHash;
    private HashTable.TableHash.Reader sourceHashReader;
    private ImmutableBytesWritable currentSourceHash;
    private ImmutableBytesWritable nextSourceKey;
    private BigtableTableHashAccessor.BigtableResultHasher targetHasher;

    private Throwable mapperException;

    public enum Counter {
      BATCHES,
      HASHES_MATCHED,
      HASHES_NOT_MATCHED,
      SOURCE_MISSING_ROWS,
      SOURCE_MISSING_CELLS,
      TARGET_MISSING_ROWS,
      TARGET_MISSING_CELLS,
      ROWS_WITH_DIFFS,
      DIFFERENT_CELL_VALUES,
      MATCHING_ROWS,
      MATCHING_CELLS,
      EMPTY_BATCHES,
      RANGES_MATCHED,
      RANGES_NOT_MATCHED
    };

    private static final NullWritable outputKey = NullWritable.get();
    private Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {

      Configuration conf = context.getConfiguration();
      sourceHashDir = new Path(conf.get(SOURCE_HASH_DIR_CONF_KEY));

      // create target connection config
      // inherit base config, but override connection based on job args
      Configuration targetConf = new Configuration(conf);
      targetConf.unset(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL);
      if (!conf.onlyKeyExists(TARGET_BT_PROJECTID_CONF_KEY)) {
        BigtableConfiguration.configure(
            targetConf,
            targetConf.get(TARGET_BT_PROJECTID_CONF_KEY),
            targetConf.get(TARGET_BT_INSTANCE_CONF_KEY),
            targetConf.get(TARGET_BT_APP_PROFILE_CONF_KEY, ""));
      }

      // create source connection config
      // inherit base config, but override connection based on job args
      Configuration sourceConf = new Configuration(conf);
      sourceConf.unset(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL);
      if (!conf.onlyKeyExists(SOURCE_BT_PROJECTID_CONF_KEY)) {
        BigtableConfiguration.configure(
            sourceConf,
            sourceConf.get(SOURCE_BT_PROJECTID_CONF_KEY),
            sourceConf.get(SOURCE_BT_INSTANCE_CONF_KEY),
            sourceConf.get(SOURCE_BT_APP_PROFILE_CONF_KEY, ""));
      }

      // create target connection using target config
      targetConnection = openConnection(targetConf, TARGET_ZK_CLUSTER_CONF_KEY);
      targetTable = openTable(targetConnection, conf, TARGET_TABLE_CONF_KEY);

      // create source connection using source config. if sourceConnection cannot be established,
      // cell-level comparison is skipped and only hash comparison is performed
      sourceConnection = null;
      sourceTable = null;
      try {
        sourceConnection = openConnection(sourceConf, SOURCE_ZK_CLUSTER_CONF_KEY);
        sourceTable = openTable(sourceConnection, conf, SOURCE_TABLE_CONF_KEY);

        // verify source connection/table
        Admin sourceAdmin = sourceConnection.getAdmin();
        sourceAdmin.tableExists(sourceTable.getName());
      } catch (Exception e) {
        // explicitly set source as null and log source unavailable
        sourceConnection = null;
        sourceTable = null;
        LOG.error(
            "Error opening connection to source with client conn impl: "
                + sourceConf.get(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL)
                + ", "
                + sourceConf.get(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY)
                + ". Source config HBase zk: "
                + sourceConf.get(SOURCE_ZK_CLUSTER_CONF_KEY)
                + ", and Bigtable project: "
                + sourceConf.get(SOURCE_BT_PROJECTID_CONF_KEY)
                + ", instance: "
                + sourceConf.get(SOURCE_BT_INSTANCE_CONF_KEY)
                + ", table: "
                + sourceConf.get(SOURCE_TABLE_CONF_KEY)
                + " with error: "
                + e.getMessage()
                + ". Processing continuing without connecting to source",
            e);
      }

      sourceTableHash = HashTable.TableHash.read(conf, sourceHashDir);
      LOG.info("Read source hash manifest: " + sourceTableHash);
      LOG.info(
          "Read "
              + BigtableTableHashAccessor.getPartitions(sourceTableHash).size()
              + " partition keys");

      TableSplit split = (TableSplit) context.getInputSplit();
      ImmutableBytesWritable splitStartKey = new ImmutableBytesWritable(split.getStartRow());

      sourceHashReader = sourceTableHash.newReader(conf, splitStartKey);
      findNextKeyHashPair();

      // create a hasher, but don't start it right away
      // instead, find the first hash batch at or after the start row
      // and skip any rows that come before.  they will be caught by the previous task
      targetHasher = new BigtableResultHasher();
    }

    /**
     * create connection to data store. if zkClusterConfKey is null or empty string, the connection
     * will default to properties set in conf for establishing connection.
     *
     * @param conf configuration for connection
     * @param zkClusterConfKey zookeeper configuration key for source or target
     * @return Connection to cluster
     * @throws IOException on connection
     */
    private static Connection openConnection(Configuration conf, String zkClusterConfKey)
        throws IOException {
      String zkCluster = conf.get(zkClusterConfKey);
      Configuration clusterConf = HBaseConfiguration.createClusterConf(conf, zkCluster);
      return ConnectionFactory.createConnection(clusterConf);
    }

    private static Table openTable(
        Connection connection, Configuration conf, String tableNameConfKey) throws IOException {
      return connection.getTable(TableName.valueOf(conf.get(tableNameConfKey)));
    }

    /**
     * Attempt to read the next source key/hash pair. If there are no more, set nextSourceKey to
     * null
     */
    private void findNextKeyHashPair() throws IOException {
      boolean hasNext = sourceHashReader.next();
      if (hasNext) {
        nextSourceKey = sourceHashReader.getCurrentKey();
      } else {
        // no more keys - last hash goes to the end
        nextSourceKey = null;
      }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      try {
        // first, finish any hash batches that end before the scanned row
        while (nextSourceKey != null && key.compareTo(nextSourceKey) >= 0) {
          moveToNextBatch(context);
        }

        // next, add the scanned row (as long as we've reached the first batch)
        if (targetHasher.isBatchStarted()) {
          targetHasher.hashResult(value);
        }
      } catch (Throwable t) {
        mapperException = t;
        Throwables.propagateIfInstanceOf(t, IOException.class);
        Throwables.propagateIfInstanceOf(t, InterruptedException.class);
        Throwables.propagate(t);
      }
    }

    /**
     * If there is an open hash batch, complete it and sync if there are diffs. Start a new batch,
     * and seek to read the
     */
    private void moveToNextBatch(Context context) throws IOException, InterruptedException {
      if (targetHasher.isBatchStarted()) {
        finishBatchAndCompareHashes(context);
      }
      targetHasher.startBatch(nextSourceKey);
      currentSourceHash = sourceHashReader.getCurrentHash();

      findNextKeyHashPair();
    }

    /**
     * Finish the currently open hash batch. Compare the target hash to the given source hash. If
     * they do not match, then sync the covered key range.
     */
    private void finishBatchAndCompareHashes(Context context)
        throws IOException, InterruptedException {

      boolean isSourceAvailable = (sourceTable != null);

      targetHasher.finishBatch();
      context.getCounter(Counter.BATCHES).increment(1);

      if (targetHasher.getBatchSize() == 0) {
        context.getCounter(Counter.EMPTY_BATCHES).increment(1);
      }
      ImmutableBytesWritable targetHash = targetHasher.getBatchHash();
      if (targetHash.equals(currentSourceHash)) {
        context.getCounter(Counter.HASHES_MATCHED).increment(1);
      } else {
        context.getCounter(Counter.HASHES_NOT_MATCHED).increment(1);

        ImmutableBytesWritable stopRow =
            nextSourceKey == null
                ? BigtableTableHashAccessor.getStopRow(sourceTableHash)
                : nextSourceKey;

        // emit range details if source is unavailable for cell-level details
        if (!isSourceAvailable) {
          outputValue.set(
              RecordFormatter.formatRangeMismatch(
                  Counter.HASHES_NOT_MATCHED,
                  toStringBinary(targetHasher.getBatchStartKey()),
                  toStringBinary(stopRow),
                  toStringBinary(currentSourceHash),
                  toStringBinary(targetHash)));
          context.write(outputKey, outputValue);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "Hash mismatch.  Key range: "
                  + toStringBinary(targetHasher.getBatchStartKey())
                  + " to "
                  + toStringBinary(stopRow)
                  + " sourceHash: "
                  + toStringBinary(currentSourceHash)
                  + " targetHash: "
                  + toStringBinary(targetHash));
        }

        syncRange(context, targetHasher.getBatchStartKey(), stopRow, isSourceAvailable);
      }
    }

    private static String toStringBinary(ImmutableBytesWritable bytes) {
      return Bytes.toStringBinary(bytes.get(), bytes.getOffset(), bytes.getLength());
    }

    private static final CellScanner EMPTY_CELL_SCANNER =
        new CellScanner(Iterators.<Result>emptyIterator());

    /**
     * Rescan the given range directly from the source and target tables. Count and log differences,
     * and output Puts and Deletes to show differences in the target table from the source table for
     * this range
     */
    private void syncRange(
        Context context,
        ImmutableBytesWritable startRow,
        ImmutableBytesWritable stopRow,
        boolean isSourceAvailable)
        throws IOException, InterruptedException {
      Scan scan = BigtableTableHashAccessor.getScan(sourceTableHash);
      scan.setStartRow(startRow.copyBytes());
      scan.setStopRow(stopRow.copyBytes());

      // skip cell-level comparison and only report hash mismatch counters if source is unavailable
      if (!isSourceAvailable) {
        LOG.error(
            "skipping cell-level comparison to determine reason code for mismatch due to sourceTable unavailable.");
        return;
      }
      ResultScanner sourceScanner = sourceTable.getScanner(scan);
      CellScanner sourceCells = new CellScanner(sourceScanner.iterator());

      ResultScanner targetScanner = targetTable.getScanner(new Scan(scan));
      CellScanner targetCells = new CellScanner(targetScanner.iterator());

      boolean rangeMatched = true;
      byte[] nextSourceRow = sourceCells.nextRow();
      byte[] nextTargetRow = targetCells.nextRow();
      while (nextSourceRow != null || nextTargetRow != null) {
        boolean rowMatched;
        int rowComparison = compareRowKeys(nextSourceRow, nextTargetRow);
        if (rowComparison < 0) {
          // target missing row
          if (LOG.isDebugEnabled()) {
            LOG.debug("Target missing row: " + Bytes.toStringBinary(nextSourceRow));
          }
          context.getCounter(Counter.TARGET_MISSING_ROWS).increment(1);

          rowMatched = syncRowCells(context, nextSourceRow, sourceCells, EMPTY_CELL_SCANNER);
          nextSourceRow = sourceCells.nextRow(); // advance only source to next row
        } else if (rowComparison > 0) {
          // source missing row
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source missing row: " + Bytes.toStringBinary(nextTargetRow));
          }
          context.getCounter(Counter.SOURCE_MISSING_ROWS).increment(1);

          rowMatched = syncRowCells(context, nextTargetRow, EMPTY_CELL_SCANNER, targetCells);
          nextTargetRow = targetCells.nextRow(); // advance only target to next row
        } else {
          // current row is the same on both sides, compare cell by cell
          rowMatched = syncRowCells(context, nextSourceRow, sourceCells, targetCells);
          nextSourceRow = sourceCells.nextRow();
          nextTargetRow = targetCells.nextRow();
        }

        if (!rowMatched) {
          rangeMatched = false;
        }
      }

      sourceScanner.close();
      targetScanner.close();

      context
          .getCounter(rangeMatched ? Counter.RANGES_MATCHED : Counter.RANGES_NOT_MATCHED)
          .increment(1);
    }

    private static class CellScanner {
      private final Iterator<Result> results;

      private byte[] currentRow;
      private Result currentRowResult;
      private int nextCellInRow;

      private Result nextRowResult;

      public CellScanner(Iterator<Result> results) {
        this.results = results;
      }

      /**
       * Advance to the next row and return its row key. Returns null iff there are no more rows.
       */
      public byte[] nextRow() {
        if (nextRowResult == null) {
          // no cached row - check scanner for more
          while (results.hasNext()) {
            nextRowResult = results.next();
            Cell nextCell = nextRowResult.rawCells()[0];
            if (currentRow == null
                || !Bytes.equals(
                    currentRow,
                    0,
                    currentRow.length,
                    nextCell.getRowArray(),
                    nextCell.getRowOffset(),
                    nextCell.getRowLength())) {
              // found next row
              break;
            } else {
              // found another result from current row, keep scanning
              nextRowResult = null;
            }
          }

          if (nextRowResult == null) {
            // end of data, no more rows
            currentRowResult = null;
            currentRow = null;
            return null;
          }
        }

        // advance to cached result for next row
        currentRowResult = nextRowResult;
        nextCellInRow = 0;
        currentRow = currentRowResult.getRow();
        nextRowResult = null;
        return currentRow;
      }

      /** Returns the next Cell in the current row or null iff none remain. */
      public Cell nextCellInRow() {
        if (currentRowResult == null) {
          // nothing left in current row
          return null;
        }

        Cell nextCell = currentRowResult.rawCells()[nextCellInRow];
        nextCellInRow++;
        if (nextCellInRow == currentRowResult.size()) {
          if (results.hasNext()) {
            Result result = results.next();
            Cell cell = result.rawCells()[0];
            if (Bytes.equals(
                currentRow,
                0,
                currentRow.length,
                cell.getRowArray(),
                cell.getRowOffset(),
                cell.getRowLength())) {
              // result is part of current row
              currentRowResult = result;
              nextCellInRow = 0;
            } else {
              // result is part of next row, cache it
              nextRowResult = result;
              // current row is complete
              currentRowResult = null;
            }
          } else {
            // end of data
            currentRowResult = null;
          }
        }
        return nextCell;
      }
    }

    /**
     * Compare the cells for the given row from the source and target tables. Count and log any
     * differences. Output a Put and/or Delete needed to sync the target table to match the source
     * table.
     */
    private boolean syncRowCells(
        Context context, byte[] rowKey, CellScanner sourceCells, CellScanner targetCells)
        throws IOException, InterruptedException {
      long matchingCells = 0;
      boolean matchingRow = true;
      Cell sourceCell = sourceCells.nextCellInRow();
      Cell targetCell = targetCells.nextCellInRow();
      while (sourceCell != null || targetCell != null) {

        int cellKeyComparison = compareCellKeysWithinRow(sourceCell, targetCell);
        if (cellKeyComparison < 0) {
          // target missing cell
          if (LOG.isDebugEnabled()) {
            LOG.debug("Target missing cell: " + sourceCell);
          }
          context.getCounter(Counter.TARGET_MISSING_CELLS).increment(1);
          outputValue.set(RecordFormatter.format(Counter.TARGET_MISSING_CELLS, sourceCell));
          context.write(outputKey, outputValue);

          matchingRow = false;
          sourceCell = sourceCells.nextCellInRow();

        } else if (cellKeyComparison > 0) {
          // source missing cell
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source missing cell: " + targetCell);
          }
          context.getCounter(Counter.SOURCE_MISSING_CELLS).increment(1);
          outputValue.set(RecordFormatter.format(Counter.SOURCE_MISSING_CELLS, targetCell));
          context.write(outputKey, outputValue);

          matchingRow = false;
          targetCell = targetCells.nextCellInRow();

        } else {
          // the cell keys are equal, now check values
          if (CellUtil.matchingValue(sourceCell, targetCell)) {
            matchingCells++;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Different values: ");
              LOG.debug(
                  "  source cell: "
                      + sourceCell
                      + " value: "
                      + Bytes.toStringBinary(
                          sourceCell.getValueArray(),
                          sourceCell.getValueOffset(),
                          sourceCell.getValueLength()));
              LOG.debug(
                  "  target cell: "
                      + targetCell
                      + " value: "
                      + Bytes.toStringBinary(
                          targetCell.getValueArray(),
                          targetCell.getValueOffset(),
                          targetCell.getValueLength()));
            }
            context.getCounter(Counter.DIFFERENT_CELL_VALUES).increment(1);
            outputValue.set(
                RecordFormatter.format(Counter.DIFFERENT_CELL_VALUES, sourceCell, targetCell));
            context.write(outputKey, outputValue);

            matchingRow = false;
          }
          sourceCell = sourceCells.nextCellInRow();
          targetCell = targetCells.nextCellInRow();
        }
      }

      if (matchingCells > 0) {
        context.getCounter(Counter.MATCHING_CELLS).increment(matchingCells);
      }
      if (matchingRow) {
        context.getCounter(Counter.MATCHING_ROWS).increment(1);
        return true;
      } else {
        context.getCounter(Counter.ROWS_WITH_DIFFS).increment(1);
        return false;
      }
    }

    private static final CellComparator cellComparator = new CellComparator();

    /** Compare row keys of the given Result objects. Nulls are after non-nulls */
    private static int compareRowKeys(byte[] r1, byte[] r2) {
      if (r1 == null) {
        return 1; // source missing row
      } else if (r2 == null) {
        return -1; // target missing row
      } else {
        return cellComparator.compareRows(r1, 0, r1.length, r2, 0, r2.length);
      }
    }

    /**
     * Compare families, qualifiers, and timestamps of the given Cells. They are assumed to be of
     * the same row. Nulls are after non-nulls.
     */
    private int compareCellKeysWithinRow(Cell c1, Cell c2) {
      if (c1 == null) {
        return 1; // source missing cell
      }
      if (c2 == null) {
        return -1; // target missing cell
      }

      int result = CellComparator.compareFamilies(c1, c2);
      if (result != 0) {
        return result;
      }

      result = CellComparator.compareQualifiers(c1, c2);
      if (result != 0) {
        return result;
      }

      // note timestamp comparison is inverted - more recent cells first
      return CellComparator.compareTimestamps(c1, c2);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      if (mapperException == null) {
        try {
          finishRemainingHashRanges(context);
        } catch (Throwable t) {
          mapperException = t;
        }
      }

      try {
        targetTable.close();
        targetConnection.close();

        if (sourceTable != null) sourceTable.close();
        if (sourceConnection != null) sourceConnection.close();
      } catch (Throwable t) {
        if (mapperException == null) {
          mapperException = t;
        } else {
          LOG.error("Suppressing exception from closing tables", t);
        }
      }

      // propagate first exception
      if (mapperException != null) {
        Throwables.propagateIfInstanceOf(mapperException, IOException.class);
        Throwables.propagateIfInstanceOf(mapperException, InterruptedException.class);
        Throwables.propagate(mapperException);
      }
    }

    private void finishRemainingHashRanges(Context context)
        throws IOException, InterruptedException {
      TableSplit split = (TableSplit) context.getInputSplit();
      byte[] splitEndRow = split.getEndRow();
      boolean reachedEndOfTable = BigtableTableHashAccessor.isTableEndRow(splitEndRow);

      // if there are more hash batches that begin before the end of this split move to them
      while (nextSourceKey != null
          && (nextSourceKey.compareTo(splitEndRow) < 0 || reachedEndOfTable)) {
        moveToNextBatch(context);
      }

      if (targetHasher.isBatchStarted()) {
        // need to complete the final open hash batch

        if ((nextSourceKey != null && nextSourceKey.compareTo(splitEndRow) > 0)
            || (nextSourceKey == null
                && !Bytes.equals(
                    splitEndRow, BigtableTableHashAccessor.getStopRow(sourceTableHash).get()))) {
          // the open hash range continues past the end of this region
          // add a scan to complete the current hash range
          Scan scan = BigtableTableHashAccessor.getScan(sourceTableHash);
          scan.setStartRow(splitEndRow);
          if (nextSourceKey == null) {
            scan.setStopRow(BigtableTableHashAccessor.getStopRow(sourceTableHash).get());
          } else {
            scan.setStopRow(nextSourceKey.copyBytes());
          }

          ResultScanner targetScanner = null;
          try {
            targetScanner = targetTable.getScanner(scan);
            for (Result row : targetScanner) {
              targetHasher.hashResult(row);
            }
          } finally {
            if (targetScanner != null) {
              targetScanner.close();
            }
          }
        } // else current batch ends exactly at split end row

        finishBatchAndCompareHashes(context);
      }
    }
  }

  /** Main entry point. */
  public static void main(String[] args) throws Exception {
    int exitCode =
        ToolRunner.run(new SyncTableValidationOnlyJob(HBaseConfiguration.create()), args);

    if (exitCode == 0) {
      System.out.println("job appears to have completed successfully.");
    } else {
      System.err.println("job is exiting with exit code='" + exitCode + "'");
    }
    System.exit(exitCode);
  }
}
