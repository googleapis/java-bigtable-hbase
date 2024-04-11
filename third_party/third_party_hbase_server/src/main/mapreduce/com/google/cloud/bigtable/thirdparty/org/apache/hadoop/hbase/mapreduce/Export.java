/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.thirdparty.org.apache.hadoop.hbase.mapreduce;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableMapper;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Export an HBase table. Writes content to sequence files up in HDFS. Use {@link
 * com.google.cloud.bigtable.mapreduce.Import} to read it back in again.
 *
 * @author sduskis
 * @version $Id: $Id
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Export {
  private static final Log LOG = LogFactory.getLog(Export.class);
  static final String NAME = "export";
  static final String RAW_SCAN = "hbase.mapreduce.include.deleted.rows";
  static final String EXPORT_BATCHING = "hbase.export.scanner.batch";
  // In addition to TableInputFormat.SCAN_COLUMN_FAMILY, which allows a user to specify a single
  // column,
  // add the ability to specify multiple column families via comma separated list
  static final String SCAN_COLUMN_FAMILIES = "hbase.mapreduce.scan.column.families";

  /**
   * Sets up the actual job.
   *
   * @param conf The current configuration.
   * @param args The command line parameters.
   * @return The newly created job.
   * @throws java.io.IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    conf.setIfUnset(
        "hbase.client.connection.impl", BigtableConfiguration.getConnectionClass().getName());
    conf.setIfUnset(BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY, "60000");
    conf.setBoolean(TableInputFormat.SHUFFLE_MAPS, true);

    String tableName = args[0];
    Path outputDir = new Path(args[1]);
    Job job = Job.getInstance(conf, NAME + "_" + tableName);
    job.setJobName(NAME + "_" + tableName);
    job.setJarByClass(Export.class);
    // Set optional scan parameters
    Scan s = getConfiguredScanForJob(conf, args);
    TableMapReduceUtil.initTableMapperJob(
        tableName,
        s,
        IdentityTableMapper.class,
        ImmutableBytesWritable.class,
        Result.class,
        job,
        false);
    // No reducers.  Just write straight to output files.
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Result.class);
    FileOutputFormat.setOutputPath(
        job, outputDir); // job conf doesn't contain the conf so doesn't have a default fs.
    return job;
  }

  private static Scan getConfiguredScanForJob(Configuration conf, String[] args)
      throws IOException {
    Scan s = new Scan();
    // Optional arguments.
    // Set Scan Versions
    int versions = args.length > 2 ? Integer.parseInt(args[2]) : 1;
    s.setMaxVersions(versions);
    // Set Scan Range
    long startTime = args.length > 3 ? Long.parseLong(args[3]) : 0L;
    long endTime = args.length > 4 ? Long.parseLong(args[4]) : Long.MAX_VALUE;
    s.setTimeRange(startTime, endTime);
    // Set cache blocks
    s.setCacheBlocks(false);
    // set Start and Stop row
    if (conf.get(TableInputFormat.SCAN_ROW_START) != null) {
      s.setStartRow(Bytes.toBytes(conf.get(TableInputFormat.SCAN_ROW_START)));
    }
    if (conf.get(TableInputFormat.SCAN_ROW_STOP) != null) {
      s.setStopRow(Bytes.toBytes(conf.get(TableInputFormat.SCAN_ROW_STOP)));
    }
    // Set Scan Column Family
    boolean raw = Boolean.parseBoolean(conf.get(RAW_SCAN));
    if (raw) {
      s.setRaw(raw);
    }

    if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
      s.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
    }
    // Add additional comma-separated families
    for (String family : conf.getStrings(SCAN_COLUMN_FAMILIES, new String[0])) {
      s.addFamily(Bytes.toBytes(family));
    }
    // Set RowFilter or Prefix Filter if applicable.
    Filter exportFilter = getExportFilter(args);
    if (exportFilter != null) {
      LOG.info("Setting Scan Filter for Export.");
      s.setFilter(exportFilter);
    }

    int batching = conf.getInt(EXPORT_BATCHING, -1);
    if (batching != -1) {
      try {
        s.setBatch(batching);
      } catch (IncompatibleFilterException e) {
        LOG.error("Batching could not be set", e);
      }
    }
    LOG.info(
        "versions="
            + versions
            + ", starttime="
            + startTime
            + ", endtime="
            + endTime
            + ", keepDeletedCells="
            + raw);
    return s;
  }

  private static Filter getExportFilter(String[] args) {
    Filter exportFilter = null;
    String filterCriteria = (args.length > 5) ? args[5] : null;
    if (filterCriteria == null) return null;
    if (filterCriteria.startsWith("^")) {
      String regexPattern = filterCriteria.substring(1, filterCriteria.length());
      exportFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(regexPattern));
    } else {
      exportFilter = new PrefixFilter(Bytes.toBytes(filterCriteria));
    }
    return exportFilter;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println(
        "Usage: Export [-D <property=value>]* <tablename> <outputdir> [<versions> "
            + "[<starttime> [<endtime>]] [^[regex pattern] or [Prefix] to filter]]\n");
    System.err.println("  Mandatory properties:");
    System.err.println("   -D " + BigtableOptionsFactory.PROJECT_ID_KEY + "=<bigtable project id>");
    System.err.println(
        "   -D " + BigtableOptionsFactory.INSTANCE_ID_KEY + "=<bigtable instance id>");
    System.err.println("  Note: -D properties will be applied to the conf used. ");
    System.err.println("  For example: ");
    System.err.println("   -D mapreduce.output.fileoutputformat.compress=true");
    System.err.println(
        "   -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec");
    System.err.println("   -D mapreduce.output.fileoutputformat.compress.type=BLOCK");
    System.err.println("  Additionally, the following SCAN properties can be specified");
    System.err.println("  to control/limit what is exported..");
    System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
    System.err.println("   -D " + SCAN_COLUMN_FAMILIES + "=<familyName>[,<familyName2>...]");
    System.err.println("   -D " + RAW_SCAN + "=true");
    System.err.println("   -D " + TableInputFormat.SCAN_ROW_START + "=<ROWSTART>");
    System.err.println("   -D " + TableInputFormat.SCAN_ROW_STOP + "=<ROWSTOP>");
    System.err.println(
        "For performance consider the following properties:\n"
            + "   -Dhbase.client.scanner.caching=100\n"
            + "   -Dmapreduce.map.speculative=false\n"
            + "   -Dmapreduce.reduce.speculative=false");
    System.err.println(
        "For tables with very wide rows consider setting the batch size as below:\n"
            + "   -D"
            + EXPORT_BATCHING
            + "=10");
  }

  /**
   * Main entry point.
   *
   * @param args The command line parameters.
   * @throws java.lang.Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }
    if (conf.get(BigtableOptionsFactory.PROJECT_ID_KEY) == null) {
      usage("Must specify the property " + BigtableOptionsFactory.PROJECT_ID_KEY);
      System.exit(-1);
    }
    if (conf.get(BigtableOptionsFactory.INSTANCE_ID_KEY) == null) {
      usage("Must specify the property" + BigtableOptionsFactory.INSTANCE_ID_KEY);
      System.exit(-1);
    }

    Job job = createSubmittableJob(conf, otherArgs);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
