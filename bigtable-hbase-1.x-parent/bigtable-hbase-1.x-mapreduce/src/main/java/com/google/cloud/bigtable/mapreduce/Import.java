/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.mapreduce;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Import data written by {@link org.apache.hadoop.hbase.mapreduce.Export}.
 *
 * @author sduskis
 * @version $Id: $Id
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Import extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(Import.class);
  static final String NAME = "import";
  /** Constant <code>CF_RENAME_PROP="HBASE_IMPORTER_RENAME_CFS"</code> */
  public static final String CF_RENAME_PROP = "HBASE_IMPORTER_RENAME_CFS";
  /** Constant <code>BULK_OUTPUT_CONF_KEY="import.bulk.output"</code> */
  public static final String BULK_OUTPUT_CONF_KEY = "import.bulk.output";
  /** Constant <code>FILTER_CLASS_CONF_KEY="import.filter.class"</code> */
  public static final String FILTER_CLASS_CONF_KEY = "import.filter.class";
  /** Constant <code>FILTER_ARGS_CONF_KEY="import.filter.args"</code> */
  public static final String FILTER_ARGS_CONF_KEY = "import.filter.args";
  /** Constant <code>TABLE_NAME="import.table.name"</code> */
  public static final String TABLE_NAME = "import.table.name";

  private static final String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  /** A mapper that just writes out KeyValues. */
  public static class KeyValueImporter extends TableMapper<ImmutableBytesWritable, KeyValue> {
    private Map<byte[], byte[]> cfRenameMap;
    private Filter filter;

    /**
     * @param row The current table row key.
     * @param value The columns.
     * @param context The current context.
     * @throws IOException When something is broken with the data.
     */
    @SuppressWarnings("deprecation")
    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
      try {
        if (LOG.isTraceEnabled()) {
          LOG.trace(
              "Considering the row." + Bytes.toString(row.get(), row.getOffset(), row.getLength()));
        }
        if (filter == null || !filter.filterRowKey(row.get(), row.getOffset(), row.getLength())) {
          for (Cell kv : value.rawCells()) {
            kv = filterKv(filter, kv);
            // skip if we filtered it out
            if (kv == null) continue;
            // TODO get rid of ensureKeyValue
            context.write(row, KeyValueUtil.ensureKeyValue(convertKv(kv, cfRenameMap)));
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("map process was interrupted", e);
      }
    }

    @Override
    public void setup(Context context) {
      cfRenameMap = createCfRenameMap(context.getConfiguration());
      filter = instantiateFilter(context.getConfiguration());
    }
  }

  /** Write table content out to files. */
  public static class Importer extends TableMapper<ImmutableBytesWritable, Mutation> {
    private Map<byte[], byte[]> cfRenameMap;
    private Filter filter;
    private Durability durability;

    /**
     * @param row The current table row key.
     * @param value The columns.
     * @param context The current context.
     * @throws IOException When something is broken with the data.
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
      try {
        writeResult(row, value, context);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("map process was interrupted", e);
      }
    }

    private void writeResult(ImmutableBytesWritable key, Result result, Context context)
        throws IOException, InterruptedException {
      Put put = null;
      Delete delete = null;
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "Considering the row." + Bytes.toString(key.get(), key.getOffset(), key.getLength()));
      }
      if (filter == null || !filter.filterRowKey(key.get(), key.getOffset(), key.getLength())) {
        processKV(key, result, context, put, delete);
      }
    }

    protected void processKV(
        ImmutableBytesWritable key, Result result, Context context, Put put, Delete delete)
        throws IOException, InterruptedException {
      for (Cell kv : result.rawCells()) {
        kv = filterKv(filter, kv);
        // skip if we filter it out
        if (kv == null) continue;

        kv = convertKv(kv, cfRenameMap);
        // Deletes and Puts are gathered and written when finished
        /*
         * If there are sequence of mutations and tombstones in an Export, and after Import the same
         * sequence should be restored as it is. If we combine all Delete tombstones into single
         * request then there is chance of ignoring few DeleteFamily tombstones, because if we
         * submit multiple DeleteFamily tombstones in single Delete request then we are maintaining
         * only newest in hbase table and ignoring other. Check - HBASE-12065
         */
        if (CellUtil.isDeleteFamily(kv)) {
          Delete deleteFamily = new Delete(key.get());
          deleteFamily.addDeleteMarker(kv);
          if (durability != null) {
            deleteFamily.setDurability(durability);
          }
          context.write(key, deleteFamily);
        } else if (CellUtil.isDelete(kv)) {
          if (delete == null) {
            delete = new Delete(key.get());
          }
          delete.addDeleteMarker(kv);
        } else {
          if (put == null) {
            put = new Put(key.get());
          }
          addPutToKv(put, kv);
        }
      }
      if (put != null) {
        if (durability != null) {
          put.setDurability(durability);
        }
        context.write(key, put);
      }
      if (delete != null) {
        if (durability != null) {
          delete.setDurability(durability);
        }
        context.write(key, delete);
      }
    }

    protected void addPutToKv(Put put, Cell kv) throws IOException {
      put.add(kv);
    }

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      cfRenameMap = createCfRenameMap(conf);
      filter = instantiateFilter(conf);
    }
  }

  /**
   * Create a {@link org.apache.hadoop.hbase.filter.Filter} to apply to all incoming keys ({@link
   * KeyValue KeyValues}) to optionally not include in the job output
   *
   * @param conf {@link org.apache.hadoop.conf.Configuration} from which to load the filter
   * @return the filter to use for the task, or <tt>null</tt> if no filter to should be used
   * @throws java.lang.IllegalArgumentException if the filter is misconfigured
   */
  public static Filter instantiateFilter(Configuration conf) {
    // get the filter, if it was configured
    Class<? extends Filter> filterClass = conf.getClass(FILTER_CLASS_CONF_KEY, null, Filter.class);
    if (filterClass == null) {
      LOG.debug("No configured filter class, accepting all keyvalues.");
      return null;
    }
    LOG.debug("Attempting to create filter:" + filterClass);
    String[] filterArgs = conf.getStrings(FILTER_ARGS_CONF_KEY);
    ArrayList<byte[]> quotedArgs = toQuotedByteArrays(filterArgs);
    try {
      Method m = filterClass.getMethod("createFilterFromArguments", ArrayList.class);
      return (Filter) m.invoke(null, quotedArgs);
    } catch (IllegalAccessException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    } catch (SecurityException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      LOG.error("Couldn't instantiate filter!", e);
      throw new RuntimeException(e);
    }
  }

  private static ArrayList<byte[]> toQuotedByteArrays(String... stringArgs) {
    ArrayList<byte[]> quotedArgs = new ArrayList<byte[]>();
    for (String stringArg : stringArgs) {
      // all the filters' instantiation methods expected quoted args since they are coming from
      // the shell, so add them here, though it shouldn't really be needed :-/
      quotedArgs.add(Bytes.toBytes("'" + stringArg + "'"));
    }
    return quotedArgs;
  }

  /**
   * Attempt to filter out the keyvalue
   *
   * @param kv {@link org.apache.hadoop.hbase.KeyValue} on which to apply the filter
   * @return <tt>null</tt> if the key should not be written, otherwise returns the original {@link
   *     org.apache.hadoop.hbase.KeyValue}
   * @param filter a {@link org.apache.hadoop.hbase.filter.Filter} object.
   * @throws java.io.IOException if any.
   */
  public static Cell filterKv(Filter filter, Cell kv) throws IOException {
    // apply the filter and skip this kv if the filter doesn't apply
    if (filter != null) {
      Filter.ReturnCode code = filter.filterKeyValue(kv);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Filter returned:" + code + " for the key value:" + kv);
      }
      // if its not an accept type, then skip this kv
      if (!(code.equals(Filter.ReturnCode.INCLUDE)
          || code.equals(Filter.ReturnCode.INCLUDE_AND_NEXT_COL))) {
        return null;
      }
    }
    return kv;
  }

  // helper: create a new KeyValue based on CF rename map
  private static Cell convertKv(Cell kv, Map<byte[], byte[]> cfRenameMap) {
    if (cfRenameMap != null) {
      // If there's a rename mapping for this CF, create a new KeyValue
      byte[] newCfName = cfRenameMap.get(CellUtil.cloneFamily(kv));
      if (newCfName != null) {
        kv =
            new KeyValue(
                kv.getRowArray(), // row buffer
                kv.getRowOffset(), // row offset
                kv.getRowLength(), // row length
                newCfName, // CF buffer
                0, // CF offset
                newCfName.length, // CF length
                kv.getQualifierArray(), // qualifier buffer
                kv.getQualifierOffset(), // qualifier offset
                kv.getQualifierLength(), // qualifier length
                kv.getTimestamp(), // timestamp
                KeyValue.Type.codeToType(kv.getTypeByte()), // KV Type
                kv.getValueArray(), // value buffer
                kv.getValueOffset(), // value offset
                kv.getValueLength()); // value length
      }
    }
    return kv;
  }

  // helper: make a map from sourceCfName to destCfName by parsing a config key
  private static Map<byte[], byte[]> createCfRenameMap(Configuration conf) {
    Map<byte[], byte[]> cfRenameMap = null;
    String allMappingsPropVal = conf.get(CF_RENAME_PROP);
    if (allMappingsPropVal != null) {
      // The conf value format should be sourceCf1:destCf1,sourceCf2:destCf2,...
      String[] allMappings = allMappingsPropVal.split(",");
      for (String mapping : allMappings) {
        if (cfRenameMap == null) {
          cfRenameMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
        }
        String[] srcAndDest = mapping.split(":");
        if (srcAndDest.length != 2) {
          continue;
        }
        cfRenameMap.put(srcAndDest[0].getBytes(), srcAndDest[1].getBytes());
      }
    }
    return cfRenameMap;
  }

  /**
   * Sets a configuration property with key {@link #CF_RENAME_PROP} in conf that tells the mapper
   * how to rename column families.
   *
   * <p>Alternately, instead of calling this function, you could set the configuration key {@link
   * #CF_RENAME_PROP} yourself. The value should look like
   *
   * <pre>srcCf1:destCf1,srcCf2:destCf2,....</pre>
   *
   * . This would have the same effect on the mapper behavior.
   *
   * @param conf the Configuration in which the {@link #CF_RENAME_PROP} key will be set
   * @param renameMap a mapping from source CF names to destination CF names
   */
  public static void configureCfRenaming(Configuration conf, Map<String, String> renameMap) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : renameMap.entrySet()) {
      String sourceCf = entry.getKey();
      String destCf = entry.getValue();

      if (sourceCf.contains(":")
          || sourceCf.contains(",")
          || destCf.contains(":")
          || destCf.contains(",")) {
        throw new IllegalArgumentException(
            "Illegal character in CF names: " + sourceCf + ", " + destCf);
      }

      if (sb.length() != 0) {
        sb.append(",");
      }
      sb.append(sourceCf + ":" + destCf);
    }
    conf.set(CF_RENAME_PROP, sb.toString());
  }

  /**
   * Add a Filter to be instantiated on import
   *
   * @param conf Configuration to update (will be passed to the job)
   * @param clazz {@link org.apache.hadoop.hbase.filter.Filter} subclass to instantiate on the
   *     server.
   * @param filterArgs List of arguments to pass to the filter on instantiation
   */
  public static void addFilterAndArguments(
      Configuration conf, Class<? extends Filter> clazz, List<String> filterArgs) {
    conf.set(Import.FILTER_CLASS_CONF_KEY, clazz.getName());
    conf.setStrings(Import.FILTER_ARGS_CONF_KEY, filterArgs.toArray(new String[filterArgs.size()]));
  }

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

    TableName tableName = TableName.valueOf(args[0]);
    conf.set(TABLE_NAME, tableName.getNameAsString());
    Path inputDir = new Path(args[1]);
    Job job = Job.getInstance(conf, conf.get(JOB_NAME_CONF_KEY, NAME + "_" + tableName));
    job.setJarByClass(Importer.class);
    FileInputFormat.setInputPaths(job, inputDir);
    // Randomize the splits to avoid hot spotting a single tablet server
    job.setInputFormatClass(ShuffledSequenceFileInputFormat.class);
    // Give the mappers enough work to do otherwise each split will be dominated by spinup time
    ShuffledSequenceFileInputFormat.setMinInputSplitSize(job, 1L * 1024 * 1024 * 1024);
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);

    // make sure we get the filter in the jars
    try {
      Class<? extends Filter> filter = conf.getClass(FILTER_CLASS_CONF_KEY, null, Filter.class);
      if (filter != null) {
        TableMapReduceUtil.addDependencyJars(conf, filter);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (hfileOutPath != null) {
      job.setMapperClass(KeyValueImporter.class);
      try (Connection conn = ConnectionFactory.createConnection(conf);
          Table table = conn.getTable(tableName);
          RegionLocator regionLocator = conn.getRegionLocator(tableName)) {
        job.setReducerClass(KeyValueSortReducer.class);
        Path outputDir = new Path(hfileOutPath);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
      }
    } else {
      // No reducers.  Just write straight to table.  Call initTableReducerJob
      // because it sets up the TableOutputFormat.
      job.setMapperClass(Importer.class);
      // TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(), null, job);
      TableMapReduceUtil.initTableReducerJob(
          tableName.getNameAsString(), null, job, null, null, null, null, false);
      job.setNumReduceTasks(0);
    }
    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: Import [options] <tablename> <inputdir>");
    System.err.println(" Mandatory properties:");
    System.err.println("  -D " + BigtableOptionsFactory.PROJECT_ID_KEY + "=<bigtable project id>");
    System.err.println(
        "  -D " + BigtableOptionsFactory.INSTANCE_ID_KEY + "=<bigtable instance id>");
    System.err.println(
        " To apply a generic org.apache.hadoop.hbase.filter.Filter to the input, use");
    System.err.println("  -D" + FILTER_CLASS_CONF_KEY + "=<name of filter class>");
    System.err.println("  -D" + FILTER_ARGS_CONF_KEY + "=<comma separated list of args for filter");
    System.err.println(
        " NOTE: The filter will be applied BEFORE doing key renames via the "
            + CF_RENAME_PROP
            + " property. Futher, filters will only use the"
            + " Filter#filterRowKey(byte[] buffer, int offset, int length) method to identify "
            + " whether the current row needs to be ignored completely for processing and "
            + " Filter#filterKeyValue(KeyValue) method to determine if the KeyValue should be added;"
            + " Filter.ReturnCode#INCLUDE and #INCLUDE_AND_NEXT_COL will be considered as including"
            + " the KeyValue.");
    System.err.println(
        "   -D "
            + JOB_NAME_CONF_KEY
            + "=jobName - use the specified mapreduce job name for the import");
    System.err.println(
        "For performance consider the following options:\n"
            + "  -Dmapreduce.map.speculative=false\n"
            + "  -Dmapreduce.reduce.speculative=false\n");
  }

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      return -1;
    }
    if (getConf().get(BigtableOptionsFactory.PROJECT_ID_KEY) == null) {
      usage("Must specify the property " + BigtableOptionsFactory.PROJECT_ID_KEY);
      return -1;
    }
    if (getConf().get(BigtableOptionsFactory.INSTANCE_ID_KEY) == null) {
      usage("Must specify the property" + BigtableOptionsFactory.INSTANCE_ID_KEY);
      return -1;
    }
    String inputVersionString = System.getProperty(ResultSerialization.IMPORT_FORMAT_VER);
    if (inputVersionString != null) {
      getConf().set(ResultSerialization.IMPORT_FORMAT_VER, inputVersionString);
    }
    Job job = createSubmittableJob(getConf(), otherArgs);
    return (job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * Main entry point.
   *
   * @param args The command line parameters.
   * @throws java.lang.Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(HBaseConfiguration.create(), new Import(), args));
  }
}
