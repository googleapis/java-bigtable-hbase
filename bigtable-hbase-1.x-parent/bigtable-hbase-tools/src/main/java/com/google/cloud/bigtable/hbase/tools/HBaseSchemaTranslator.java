/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.hbase.tools;

import com.google.bigtable.repackaged.com.google.common.base.Preconditions;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.tools.ClusterSchemaDefinition.TableSchemaDefinition;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.htrace.fasterxml.jackson.databind.ObjectWriter;

public class HBaseSchemaTranslator {

  public static final String PROJECT_ID_KEY = "google.bigtable.project.id";
  public static final String INSTANCE_ID_KEY = "google.bigtable.instance.id";
  public static final String ZOOKEEPER_QUORUM_KEY = "hbase.zookeeper.quorum";
  public static final String ZOOKEEPER_PORT_KEY = "hbase.zookeeper.property.clientPort";
  public static final String INPUT_FILE_KEY = "google.bigtable.input.filepath";
  public static final String OUTPUT_FILE_KEY = "google.bigtable.output.filepath";
  public static final String TABLE_NAME_FILTER_KEY = "google.bigtable.table.filter";

  private SchemaReadingStrategy readingStrategy;
  private SchemaWritingStrategy writingStrategy;
  // TODO Add a customizing strategy.

  static class SchemaTranslationOptions {

    String projectId;
    String instanceId;
    String zookeeperQuorum;
    Integer zookeeperPort;
    String inputFilePath;
    String outputFilePath;
    String tableNameFilter;

    @VisibleForTesting
    SchemaTranslationOptions() {}

    @VisibleForTesting
    void validateOptions() {
      if (outputFilePath != null) {
        Preconditions.checkArgument(
            projectId == null && instanceId == null,
            INSTANCE_ID_KEY + "/" + PROJECT_ID_KEY + " can not be set when output file is set.");
      } else {
        Preconditions.checkArgument(
            projectId != null && instanceId != null,
            "Schema destination not specified. "
                + PROJECT_ID_KEY
                + " and "
                + INSTANCE_ID_KEY
                + " are both required for creating table in Cloud Bigtable. "
                + INPUT_FILE_KEY
                + " is required for writing schema to a file.");
      }

      if (inputFilePath != null) {
        Preconditions.checkArgument(
            zookeeperPort == null && zookeeperQuorum == null,
            ZOOKEEPER_PORT_KEY
                + "/"
                + ZOOKEEPER_QUORUM_KEY
                + " can not be set when input file is set.");
        Preconditions.checkArgument(
            tableNameFilter == null,
            TABLE_NAME_FILTER_KEY
                + " is not supported for reading the schema from a table. "
                + "TableFilter should be used when writing the schema to the file.");
      } else {
        Preconditions.checkArgument(
            zookeeperQuorum != null && zookeeperPort != null,
            "No schema source specified. "
                + ZOOKEEPER_PORT_KEY
                + " and "
                + ZOOKEEPER_QUORUM_KEY
                + " are both required for reading schema from HBase. "
                + INPUT_FILE_KEY
                + " is required to read schema from a file.");
      }
    }

    public static SchemaTranslationOptions loadOptionsFromSystemProperties() {
      SchemaTranslationOptions options = new SchemaTranslationOptions();
      options.projectId = System.getProperty(PROJECT_ID_KEY);
      options.instanceId = System.getProperty(INSTANCE_ID_KEY);
      options.outputFilePath = System.getProperty(OUTPUT_FILE_KEY);
      options.inputFilePath = System.getProperty(INPUT_FILE_KEY);
      options.zookeeperQuorum = System.getProperty(ZOOKEEPER_QUORUM_KEY);
      if (System.getProperty(ZOOKEEPER_PORT_KEY) != null) {
        options.zookeeperPort = Integer.parseInt(System.getProperty(ZOOKEEPER_PORT_KEY));
      }

      options.tableNameFilter = System.getProperty(TABLE_NAME_FILTER_KEY);

      // Ensure that the options are set properly
      // TODO It is possible to validate the options without creating the object, but its less
      // readable. See if we can make it readable and validate before calling the constructor.
      options.validateOptions();

      return options;
    }
  }

  // All strategies should be static and stateless.
  interface SchemaReadingStrategy {

    ClusterSchemaDefinition readSchema() throws IOException;
  }

  static class FileBasedSchemaReadingStrategy implements SchemaReadingStrategy {

    String schemaFilePath;

    public FileBasedSchemaReadingStrategy(String schemaFilePath) {
      this.schemaFilePath = schemaFilePath;
    }

    @Override
    public ClusterSchemaDefinition readSchema() throws IOException {
      return new ObjectMapper().readValue(new File(schemaFilePath), ClusterSchemaDefinition.class);
    }
  }

  static class HBaseSchemaReadingStrategy implements SchemaReadingStrategy {
    private final String tableFilterPattern;
    private final Admin hbaseAdmin;

    public HBaseSchemaReadingStrategy(
        String zookeeperQuorum, int zookeeperPort, String tableFilterPattern) throws IOException {

      // If no filter is provided, use `.*` to match all the tables.
      this.tableFilterPattern = tableFilterPattern == null ? ".*" : tableFilterPattern;

      // Create the HBase admin client.
      Configuration conf = HBaseConfiguration.create();
      conf.set(ZOOKEEPER_PORT_KEY, zookeeperPort + "");
      conf.set(ZOOKEEPER_QUORUM_KEY, zookeeperQuorum);
      Connection connection = ConnectionFactory.createConnection(conf);
      this.hbaseAdmin = connection.getAdmin();
    }

    @VisibleForTesting
    HBaseSchemaReadingStrategy(Admin admin, String tableFilterPattern) {
      this.hbaseAdmin = admin;
      // If no filter is provided, use `.*` to match all the tables.
      this.tableFilterPattern = tableFilterPattern == null ? ".*" : tableFilterPattern;
    }

    private List<HTableDescriptor> getTables() throws IOException {

      System.out.println("Reading tables from " + hbaseAdmin.getClass().getCanonicalName());
      // Read the table definitions
      HTableDescriptor[] tables = null;
      try {
        // tables = hbaseAdmin.listTables(".*");
        // TODO reuse the patterns.
        tables = hbaseAdmin.listTables(tableFilterPattern);
      } catch (Exception e) {
        System.out.println(" ERROR in Getting tables from Hbase: " + e.getMessage());
        throw e;
      }
      if (tables == null) {
        System.out.println(" Found no tables");
        return new LinkedList<>();
      }
      System.out.println("Listing tables " + Arrays.asList(tables));
      return Arrays.asList(tables);
    }

    private byte[][] getSplits(TableName table) throws IOException {
      List<HRegionInfo> regions = null;
      try {
        regions = hbaseAdmin.getTableRegions(table);
      } catch (IOException e) {
        System.out.println(" ERROR in Getting splits from Hbase: " + e.getMessage());
        throw e;
      }

      if (regions == null || regions.isEmpty()) {
        return new byte[0][];
      }

      // An Infinity (EMPTY_BYTE_ARRAY) region split is not required. Bigtable splits will always
      // start from EMPTY_BYTE_ARRAY.
      // TODO: is the regions list always sorted? If not, the first region with "" start key will
      // not always be the first element.
      int numSplits = regions.get(0).getStartKey().length > 0 ? regions.size() : regions.size() - 1;
      byte[][] splits = new byte[numSplits][];
      int i = 0;
      for (HRegionInfo region : regions) {
        // TODO If regions are sorted then no need to check it every time.
        if (region.getStartKey().length > 0) {
          splits[i] = region.getStartKey();
          i++;
        }
      }
      System.out.println("Listing splits " + Arrays.asList(splits));
      return splits;
    }

    @Override
    public ClusterSchemaDefinition readSchema() throws IOException {
      System.out.println("Reading schema from HBase: ");
      ClusterSchemaDefinition schemaDefinition = new ClusterSchemaDefinition();
      List<HTableDescriptor> tables = getTables();
      System.out.println("Found tables: " + tables);
      for (HTableDescriptor table : tables) {
        byte[][] splits = getSplits(table.getTableName());
        // TODO Maybe create a addTableSchema(table, splits) method?
        schemaDefinition.tableSchemaDefinitions.add(new TableSchemaDefinition(table, splits));
      }
      return schemaDefinition;
    }
  }

  interface SchemaWritingStrategy {

    void writeSchema(ClusterSchemaDefinition schemaDefinition) throws IOException;
  }

  static class FileBasedSchemaWritingStrategy implements SchemaWritingStrategy {

    String outputFilePath;

    public FileBasedSchemaWritingStrategy(String outputFilePath) {
      this.outputFilePath = outputFilePath;
    }

    @Override
    public void writeSchema(ClusterSchemaDefinition schemaDefinition) throws IOException {
      ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
      writer.writeValue(new File(outputFilePath), schemaDefinition);
      System.out.println("Wrote schema to file " + outputFilePath);
    }
  }

  static class BigtableBasedSchemaWritingStrategy implements SchemaWritingStrategy {

    private final Admin btAdmin;

    public BigtableBasedSchemaWritingStrategy(String projectId, String instanceId)
        throws IOException {
      Configuration btConf = BigtableConfiguration.configure(projectId, instanceId);
      this.btAdmin = ConnectionFactory.createConnection(btConf).getAdmin();
    }

    @VisibleForTesting
    BigtableBasedSchemaWritingStrategy(Admin btAdmin) {
      this.btAdmin = btAdmin;
    }

    @Override
    public void writeSchema(ClusterSchemaDefinition schemaDefinition) throws IOException {
      for (TableSchemaDefinition tableSchemaDefinition : schemaDefinition.tableSchemaDefinitions) {
        String tableName = tableSchemaDefinition.name;
        try {
          btAdmin.createTable(
              tableSchemaDefinition.getHbaseTableDescriptor(), tableSchemaDefinition.splits);
          System.out.println("Successfully created table " + tableName + "  in Bigtable cluster.");
        } catch (Exception e) {
          System.err.println("failed to create table " + tableName);
          e.printStackTrace();
          // Continue creating tables in BT. Skipping creation failures makes the script idempotent
          // as BT will throw TableExistsException for a table that is already present.
        }
      }
    }
  }

  public HBaseSchemaTranslator(SchemaTranslationOptions options) throws IOException {
    Preconditions.checkNotNull(options, "SchemaTranslationOptions can not be null.");
    if (options.inputFilePath != null) {
      this.readingStrategy = new FileBasedSchemaReadingStrategy(options.inputFilePath);
    } else {
      this.readingStrategy =
          new HBaseSchemaReadingStrategy(
              options.zookeeperQuorum, options.zookeeperPort, options.tableNameFilter);
    }

    if (options.outputFilePath != null) {
      this.writingStrategy = new FileBasedSchemaWritingStrategy(options.outputFilePath);
    } else {
      this.writingStrategy =
          new BigtableBasedSchemaWritingStrategy(options.projectId, options.instanceId);
    }
  }

  @VisibleForTesting
  HBaseSchemaTranslator(
      SchemaReadingStrategy readingStrategy, SchemaWritingStrategy writingStrategy) {
    this.readingStrategy = readingStrategy;
    this.writingStrategy = writingStrategy;
  }

  public void translate() throws IOException {
    ClusterSchemaDefinition schemaDefinition = readingStrategy.readSchema();
    System.out.println("Found schema: " + schemaDefinition);
    this.writingStrategy.writeSchema(schemaDefinition);
  }

  // -Dgoogle.bigtable.project.id="google.com:cloud-bigtable-dev"
  // -Dgoogle.bigtable.instance.id="shitanshu-test"
  // -Dgoogle.bigtable.input.filepath="/tmp/schemaHbase.json"
  // -Dgoogle.bigtable.output.filepath="/tmp/schemaHbase.json"
  // -Dgoogle.bigtable.table.filter="hbase.*" -Dhbase.zookeeper.property.clientPort="2181"
  // -Dhbase.zookeeper.quorum="127.0.0.1"
  public static void main(String[] args) throws IOException {
    SchemaTranslationOptions options = SchemaTranslationOptions.loadOptionsFromSystemProperties();
    HBaseSchemaTranslator translator = new HBaseSchemaTranslator(options);
    translator.translate();
  }
}
