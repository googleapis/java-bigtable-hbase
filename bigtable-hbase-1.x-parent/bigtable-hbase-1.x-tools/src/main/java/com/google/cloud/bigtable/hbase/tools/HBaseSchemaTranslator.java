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

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import com.google.bigtable.repackaged.com.google.common.base.Preconditions;
import com.google.bigtable.repackaged.com.google.gson.Gson;
import com.google.bigtable.repackaged.javax.annotation.Nullable;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.tools.ClusterSchemaDefinition.TableSchemaDefinition;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.ArrayList;
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
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to create tables in Cloud Bigtable based on the tables in an HBase cluster.
 *
 * <p>Execute the following command to copy the schema from HBase to Cloud Bigtable:
 *
 * <pre>
 * java -jar bigtable-hbase-tools-1.14.1-SNAPSHOT-jar-with-dependencies.jar com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator \
 *  -Dhbase.zookeeper.quorum=$ZOOKEEPER_QUORUM \
 *  -Dhbase.zookeeper.property.clientPort=$ZOOKEEPER_PORT \
 *  -Dgoogle.bigtable.table.filter=$TABLE_NAME_REGEX \
 *  -Dgoogle.bigtable.project.id=$PROJECT_ID \
 *  -Dgoogle.bigtable.instance.id=$INSTANCE_ID
 * </pre>
 *
 * <p>There are 2 ways to run this tool. If you can connect to both HBase and Cloud Bigtable, you
 * can use the above method to create tables in Cloud Bigtable directly. However, if HBase master is
 * in a private VPC or can't connect to internet, you can dump HBase schema in a file and create
 * tables in Cloud Bigtable using that file.
 *
 * <p>Run the tool from a host that can connect to HBase. Store HBase schema in a file:
 *
 * <pre>
 * java -jar bigtable-hbase-tools-1.14.1-SNAPSHOT-jar-with-dependencies.jar com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator \
 *  -Dhbase.zookeeper.quorum=$ZOOKEEPER_QUORUM \
 *  -Dhbase.zookeeper.property.clientPort=$ZOOKEEPER_PORT \
 *  -Dgoogle.bigtable.table.filter=$TABLE_NAME_REGEX \
 *  -Dgoogle.bigtable.output.filepath=$SCHEMA_FILE_PATH
 * </pre>
 *
 * <p>Copy the schema file to a host which can connect to Google Cloud. Create tables in Cloud
 * Bigtable using the schema file:
 *
 * <pre>
 * java -jar bigtable-hbase-tools-1.14.1-SNAPSHOT-jar-with-dependencies.jar com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator \
 *  -Dgoogle.bigtable.input.filepath=$SCHEMA_FILE_PATH \
 *  -Dgoogle.bigtable.project.id=$PROJECT_ID \
 *  -Dgoogle.bigtable.instance.id=$INSTANCE_ID
 * </pre>
 */
@InternalApi
public class HBaseSchemaTranslator {

  public static final String PROJECT_ID_KEY = "google.bigtable.project.id";
  public static final String INSTANCE_ID_KEY = "google.bigtable.instance.id";
  public static final String ZOOKEEPER_QUORUM_KEY = "hbase.zookeeper.quorum";
  public static final String ZOOKEEPER_PORT_KEY = "hbase.zookeeper.property.clientPort";
  public static final String INPUT_FILE_KEY = "google.bigtable.input.filepath";
  public static final String OUTPUT_FILE_KEY = "google.bigtable.output.filepath";
  public static final String TABLE_NAME_FILTER_KEY = "google.bigtable.table.filter";

  private static Logger LOG = LoggerFactory.getLogger(HBaseSchemaTranslator.class);

  private final SchemaReader schemaReader;
  private final SchemaWriter schemaWriter;
  // TODO Add a schemaOverrider

  @VisibleForTesting
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
            projectId != null && instanceId != null, "Schema destination not specified.");
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
            zookeeperQuorum != null && zookeeperPort != null, "Schema source not specified. ");
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
      try {
        options.validateOptions();
      } catch (Exception e) {
        usage(e.getMessage());
        throw e;
      }
      return options;
    }
  }

  /** Interface for reading HBase schema. */
  interface SchemaReader {

    ClusterSchemaDefinition readSchema() throws IOException;
  }

  /**
   * Reads HBase schema from a JSON file. JSON file should be representation of a {@link
   * ClusterSchemaDefinition} object.
   */
  static class FileBasedSchemaReader implements SchemaReader {

    private final String schemaFilePath;

    public FileBasedSchemaReader(String schemaFilePath) {
      this.schemaFilePath = schemaFilePath;
    }

    @Override
    public ClusterSchemaDefinition readSchema() throws IOException {
      Reader jsonReader = new FileReader(schemaFilePath);
      return new Gson().fromJson(jsonReader, ClusterSchemaDefinition.class);
    }
  }

  /** Reads the HBase schema by connecting to an HBase cluster. */
  static class HBaseSchemaReader implements SchemaReader {

    private final String tableFilterPattern;
    private final Admin hbaseAdmin;

    public HBaseSchemaReader(
        String zookeeperQuorum, int zookeeperPort, @Nullable String tableFilterPattern)
        throws IOException {

      // If no filter is provided, use `.*` to match all the tables.
      this.tableFilterPattern = tableFilterPattern == null ? ".*" : tableFilterPattern;

      // Create the HBase admin client.
      Configuration conf = HBaseConfiguration.create();
      conf.setInt(ZOOKEEPER_PORT_KEY, zookeeperPort);
      conf.set(ZOOKEEPER_QUORUM_KEY, zookeeperQuorum);
      Connection connection = ConnectionFactory.createConnection(conf);
      this.hbaseAdmin = connection.getAdmin();
    }

    @VisibleForTesting
    HBaseSchemaReader(Admin admin, @Nullable String tableFilterPattern) {
      this.hbaseAdmin = admin;
      // If no filter is provided, use `.*` to match all the tables.
      this.tableFilterPattern = tableFilterPattern == null ? ".*" : tableFilterPattern;
    }

    private List<HTableDescriptor> getTables() throws IOException {
      // Read the table definitions
      HTableDescriptor[] tables = hbaseAdmin.listTables(tableFilterPattern);

      if (tables == null) {
        LOG.info(" Found no tables");
        return new LinkedList<>();
      }
      return Arrays.asList(tables);
    }

    private byte[][] getSplits(TableName table) throws IOException {
      List<HRegionInfo> regions = hbaseAdmin.getTableRegions(table);

      if (regions == null || regions.isEmpty()) {
        return new byte[0][];
      }

      byte[][] splits = new byte[regions.size()][];
      int i = 0;
      for (HRegionInfo region : regions) {
        splits[i] = region.getStartKey();
        i++;
      }
      LOG.debug("Found {} splits for table {}.", splits.length, table.getNameAsString());
      return splits;
    }

    @Override
    public ClusterSchemaDefinition readSchema() throws IOException {
      LOG.info("Reading schema from HBase.");
      ClusterSchemaDefinition schemaDefinition = new ClusterSchemaDefinition();
      List<HTableDescriptor> tables = getTables();
      for (HTableDescriptor table : tables) {
        LOG.debug("Found table {} in HBase.", table.getNameAsString());
        LOG.trace("Table details: {}", table);

        schemaDefinition.addTableSchemaDefinition(table, getSplits(table.getTableName()));
      }
      return schemaDefinition;
    }
  }

  /**
   * Interface for writing the HBase schema represented by a {@link ClusterSchemaDefinition} object.
   */
  interface SchemaWriter {

    void writeSchema(ClusterSchemaDefinition schemaDefinition) throws IOException;
  }

  /**
   * Writes the HBase schema into a file. File contains the JSON representation of the {@link
   * ClusterSchemaDefinition} object.
   */
  static class FileBasedSchemaWriter implements SchemaWriter {

    private final String outputFilePath;

    public FileBasedSchemaWriter(String outputFilePath) {
      this.outputFilePath = outputFilePath;
    }

    @Override
    public void writeSchema(ClusterSchemaDefinition schemaDefinition) throws IOException {
      Preconditions.checkNotNull(schemaDefinition, "SchemaDefinitions can not be null.");
      try (Writer writer = new FileWriter(outputFilePath)) {
        new Gson().toJson(schemaDefinition, writer);
        LOG.info("Wrote schema to file " + outputFilePath);
      }
    }
  }

  /**
   * Creates tables in Cloud Bigtable based on the schema provided by the {@link
   * ClusterSchemaDefinition} object.
   */
  static class BigtableSchemaWriter implements SchemaWriter {

    private final Admin btAdmin;

    public BigtableSchemaWriter(String projectId, String instanceId) throws IOException {
      Configuration btConf = BigtableConfiguration.configure(projectId, instanceId);
      btConf.set(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, "HBaseSchemaTranslator");
      this.btAdmin = ConnectionFactory.createConnection(btConf).getAdmin();
    }

    @VisibleForTesting
    BigtableSchemaWriter(Admin btAdmin) {
      this.btAdmin = btAdmin;
    }

    @Override
    public void writeSchema(ClusterSchemaDefinition schemaDefinition) {
      Preconditions.checkNotNull(schemaDefinition, "SchemaDefinitions can not be null.");
      List<String> failedTables = new ArrayList<>();
      for (TableSchemaDefinition tableSchemaDefinition : schemaDefinition.tableSchemaDefinitions) {
        String tableName = tableSchemaDefinition.name;
        try {
          btAdmin.createTable(
              tableSchemaDefinition.getHbaseTableDescriptor(), tableSchemaDefinition.splits);
          LOG.info("Created table {} in Bigtable.", tableName);
        } catch (Exception e) {
          failedTables.add(tableName);
          LOG.error("Failed to create table {}.", e, tableName);
          // Continue creating tables in BT. Skipping creation failures makes the script idempotent
          // as BT will throw TableExistsException for a table that is already present.
        }
      }
      if (!failedTables.isEmpty()) {
        throw new RuntimeException(
            "Failed to create some tables in Cloud Bigtable: " + failedTables);
      }
    }
  }

  public HBaseSchemaTranslator(SchemaTranslationOptions options) throws IOException {
    Preconditions.checkNotNull(options, "SchemaTranslationOptions can not be null.");
    if (options.inputFilePath != null) {
      this.schemaReader = new FileBasedSchemaReader(options.inputFilePath);
    } else {
      this.schemaReader =
          new HBaseSchemaReader(
              options.zookeeperQuorum, options.zookeeperPort, options.tableNameFilter);
    }

    if (options.outputFilePath != null) {
      this.schemaWriter = new FileBasedSchemaWriter(options.outputFilePath);
    } else {
      this.schemaWriter = new BigtableSchemaWriter(options.projectId, options.instanceId);
    }
  }

  @VisibleForTesting
  HBaseSchemaTranslator(SchemaReader schemaReader, SchemaWriter schemaWriter) {
    this.schemaReader = schemaReader;
    this.schemaWriter = schemaWriter;
  }

  public void translate() throws IOException {
    ClusterSchemaDefinition schemaDefinition = schemaReader.readSchema();
    LOG.info("Read schema with {} tables.", schemaDefinition.tableSchemaDefinitions.size());
    this.schemaWriter.writeSchema(schemaDefinition);
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    // Print usage on system.err instead of logger.
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    String jarName;
    try {
      jarName =
          new File(
                  HBaseSchemaTranslator.class
                      .getProtectionDomain()
                      .getCodeSource()
                      .getLocation()
                      .toURI()
                      .getPath())
              .getName();
    } catch (URISyntaxException e) {
      jarName = "<jar>";
    }

    System.err.printf(
        "Usage: java -jar %s com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator "
            + "<schema_source> <schema_destination> <table-name-regex> \n\n",
        jarName);
    System.err.println("  Schema Source can be 1 of the following:");
    System.err.println(
        "   -D "
            + ZOOKEEPER_QUORUM_KEY
            + "=<zookeeper quorum> -D "
            + ZOOKEEPER_PORT_KEY
            + "=<zookeeper port>");
    System.err.println("   -D " + INPUT_FILE_KEY + "=<schema file path>");
    System.err.println("  Schema destination can be 1 of the following:");
    System.err.println(
        "   -D "
            + PROJECT_ID_KEY
            + "=<bigtable project id> -D "
            + INSTANCE_ID_KEY
            + "=<bigtable instance id>");
    System.err.println("   -D " + OUTPUT_FILE_KEY + "=<schema file path>");
    System.err.println(
        "  Additionally, you can filter tables to create when using HBase as source");
    System.err.println("   -D " + TABLE_NAME_FILTER_KEY + "=<table name regex>");
  }

  public static void main(String[] args) throws IOException {
    // Configure the logger.
    BasicConfigurator.configure();

    SchemaTranslationOptions options = SchemaTranslationOptions.loadOptionsFromSystemProperties();
    HBaseSchemaTranslator translator = new HBaseSchemaTranslator(options);
    translator.translate();
  }
}
