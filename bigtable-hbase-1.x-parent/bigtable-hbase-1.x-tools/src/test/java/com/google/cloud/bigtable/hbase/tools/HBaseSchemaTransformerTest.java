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
package com.google.cloud.bigtable.hbase.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.bigtable.repackaged.com.google.gson.Gson;
import com.google.bigtable.repackaged.com.google.gson.JsonObject;
import com.google.cloud.bigtable.hbase.tools.ClusterSchemaDefinition.TableSchemaDefinition;
import com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator.JsonBasedSchemaTransformer;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HBaseSchemaTransformerTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private String schemaFilePath;
  private JsonBasedSchemaTransformer schemaTransformer;

  @Before
  public void before() throws IOException {
    schemaFilePath = tempFolder.newFile().getPath();
  }

  ///////////////////////////////// Invalid cases //////////////////////////////////////
  @Test
  public void testBadJson() throws IOException {

    // Setup bad JSON
    FileWriter fw = new FileWriter(schemaFilePath);
    fw.write("random JSON string");
    // Execute the test method
    try {
      schemaTransformer =
          JsonBasedSchemaTransformer.newSchemaTransformerFromJsonFile(schemaFilePath);
      fail("Expected IllegalStateException due to bad JSON.");
    } catch (IllegalStateException e) {
      e.printStackTrace();
    } catch (Exception e) {
      fail("Expected IllegalStateException due to bad JSON but found " + e.toString());
    }
  }

  //////////////////////////////////////// Happy cases ////////////////////////////////////
  @Test
  public void testParseJsonTables() throws IOException {
    // Setup the schema file
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("old-table", "new-table");
    // Make sure that the whole table name is matched and not just prefix
    jsonObject.addProperty("old-table-2", "random-table-2");
    try (Writer writer = new FileWriter(schemaFilePath)) {
      new Gson().toJson(jsonObject, writer);
    }

    schemaTransformer = JsonBasedSchemaTransformer.newSchemaTransformerFromJsonFile(schemaFilePath);

    // Validate
    Map<String, String> expectedTableMapping = new HashMap<>();
    expectedTableMapping.put("old-table", "new-table");
    expectedTableMapping.put( "old-table-2", "random-table-2");
    assertEquals(expectedTableMapping, schemaTransformer.tableNameMappings);
  }

  @Test
  public void testParseJsonEmpty() throws IOException {
    // Setup the schema file
    JsonObject jsonObject = new JsonObject();
    try (Writer writer = new FileWriter(schemaFilePath)) {
      new Gson().toJson(jsonObject, writer);
    }

    // Parse schema file
    schemaTransformer = JsonBasedSchemaTransformer.newSchemaTransformerFromJsonFile(schemaFilePath);
    assertEquals(0, schemaTransformer.tableNameMappings.size());
  }

  @Test
  public void testTransformWithNormalTable() throws DeserializationException, IOException {

    // Setup a cluster schema
    ClusterSchemaDefinition schemaDefinition = new ClusterSchemaDefinition();
    TableSchemaDefinition tableSchemaDefinition = new TableSchemaDefinition();
    tableSchemaDefinition.name = "test-table1";
    tableSchemaDefinition.splits = new byte[3][];
    tableSchemaDefinition.splits[0] = HConstants.EMPTY_BYTE_ARRAY;
    tableSchemaDefinition.splits[1] = "first-split".getBytes();
    tableSchemaDefinition.splits[2] = "second-split".getBytes();
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("test-table1"));
    HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
    columnDescriptor.setMaxVersions(2).setTimeToLive(1000);
    tableDescriptor.addFamily(columnDescriptor);
    tableSchemaDefinition.tableDescriptor = tableDescriptor.toByteArray();
    schemaDefinition.tableSchemaDefinitions.add(tableSchemaDefinition);

    // setup a transformer
    Map<String, String> schemaMapping = new HashMap<>();
    // NOOP transform
    schemaMapping.put("test-table1", "test-table1");
    JsonBasedSchemaTransformer transformer = new JsonBasedSchemaTransformer(schemaMapping);

    ClusterSchemaDefinition transformedClusterSchema = transformer.transform(schemaDefinition);

    // Verify transformation
    Assert.assertEquals(
        transformedClusterSchema.tableSchemaDefinitions.get(0).name,
        schemaDefinition.tableSchemaDefinitions.get(0).name);
    Assert.assertEquals(
        transformedClusterSchema.tableSchemaDefinitions.get(0).getHbaseTableDescriptor(),
        schemaDefinition.tableSchemaDefinitions.get(0).getHbaseTableDescriptor());
    Assert.assertEquals(
        transformedClusterSchema.tableSchemaDefinitions.get(0).splits,
        schemaDefinition.tableSchemaDefinitions.get(0).splits);
  }

  @Test
  public void testTransformWithEmptyMappings() throws DeserializationException, IOException {

    // Setup a cluster schema
    ClusterSchemaDefinition schemaDefinition = new ClusterSchemaDefinition();
    TableSchemaDefinition tableSchemaDefinition = new TableSchemaDefinition();
    tableSchemaDefinition.name = "test-table1";
    tableSchemaDefinition.splits = new byte[3][];
    tableSchemaDefinition.splits[0] = HConstants.EMPTY_BYTE_ARRAY;
    tableSchemaDefinition.splits[1] = "first-split".getBytes();
    tableSchemaDefinition.splits[2] = "second-split".getBytes();
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("test-table1"));
    HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
    columnDescriptor.setMaxVersions(2).setTimeToLive(1000);
    tableDescriptor.addFamily(columnDescriptor);
    tableSchemaDefinition.tableDescriptor = tableDescriptor.toByteArray();
    schemaDefinition.tableSchemaDefinitions.add(tableSchemaDefinition);

    // setup a transformer
    Map<String, String> schemaMapping = new HashMap<>();
    JsonBasedSchemaTransformer transformer = new JsonBasedSchemaTransformer(schemaMapping);

    ClusterSchemaDefinition transformedClusterSchema = transformer.transform(schemaDefinition);

    // Verify transformation
    Assert.assertEquals(
        transformedClusterSchema.tableSchemaDefinitions.get(0).name,
        schemaDefinition.tableSchemaDefinitions.get(0).name);
    Assert.assertEquals(
        transformedClusterSchema.tableSchemaDefinitions.get(0).getHbaseTableDescriptor(),
        schemaDefinition.tableSchemaDefinitions.get(0).getHbaseTableDescriptor());
    Assert.assertEquals(
        transformedClusterSchema.tableSchemaDefinitions.get(0).splits,
        schemaDefinition.tableSchemaDefinitions.get(0).splits);
  }

  @Test
  public void testTransformWithExtraMapping() throws DeserializationException, IOException {

    // Setup a cluster schema
    ClusterSchemaDefinition schemaDefinition = new ClusterSchemaDefinition();
    TableSchemaDefinition tableSchemaDefinition = new TableSchemaDefinition();
    tableSchemaDefinition.name = "test-table1";
    tableSchemaDefinition.splits = new byte[3][];
    tableSchemaDefinition.splits[0] = HConstants.EMPTY_BYTE_ARRAY;
    tableSchemaDefinition.splits[1] = "first-split".getBytes();
    tableSchemaDefinition.splits[2] = "second-split".getBytes();
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("test-table1"));
    HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
    columnDescriptor.setMaxVersions(2).setTimeToLive(1000);
    tableDescriptor.addFamily(columnDescriptor);
    tableSchemaDefinition.tableDescriptor = tableDescriptor.toByteArray();
    schemaDefinition.tableSchemaDefinitions.add(tableSchemaDefinition);

    // setup a transformer
    Map<String, String> schemaMapping = new HashMap<>();
    // NOOP transform
    schemaMapping.put("test-table1", "new-table1");
    schemaMapping.put("test", "new-table2");
    JsonBasedSchemaTransformer transformer = new JsonBasedSchemaTransformer(schemaMapping);

    ClusterSchemaDefinition transformedClusterSchema = transformer.transform(schemaDefinition);
    TableSchemaDefinition expectedTableSchema =
        new TableSchemaDefinition(
            new HTableDescriptor(
                TableName.valueOf("new-table1"),
                schemaDefinition.tableSchemaDefinitions.get(0).getHbaseTableDescriptor()),
            tableSchemaDefinition.splits);

    // Verify transformation
    Assert.assertEquals("new-table1", transformedClusterSchema.tableSchemaDefinitions.get(0).name);
    Assert.assertEquals(
        expectedTableSchema.getHbaseTableDescriptor(),
        transformedClusterSchema.tableSchemaDefinitions.get(0).getHbaseTableDescriptor());
    Assert.assertEquals(
        transformedClusterSchema.tableSchemaDefinitions.get(0).splits,
        schemaDefinition.tableSchemaDefinitions.get(0).splits);
  }

  @Test
  public void testTransformWithinCustomNamespaceTable()
      throws DeserializationException, IOException {

    // Setup a cluster schema
    ClusterSchemaDefinition schemaDefinition = new ClusterSchemaDefinition();
    TableSchemaDefinition tableSchemaDefinition = new TableSchemaDefinition();
    tableSchemaDefinition.name = "ns:test-table1";
    tableSchemaDefinition.splits = new byte[3][];
    tableSchemaDefinition.splits[0] = HConstants.EMPTY_BYTE_ARRAY;
    tableSchemaDefinition.splits[1] = "first-split".getBytes();
    tableSchemaDefinition.splits[2] = "second-split".getBytes();
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("ns", "test-table1"));
    HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
    columnDescriptor.setMaxVersions(2).setTimeToLive(1000);
    tableDescriptor.addFamily(columnDescriptor);
    tableSchemaDefinition.tableDescriptor = tableDescriptor.toByteArray();
    schemaDefinition.tableSchemaDefinitions.add(tableSchemaDefinition);

    // setup a transformer
    Map<String, String> schemaMapping = new HashMap<>();
    // NOOP transform
    schemaMapping.put("ns:test-table1", "new-table1");
    JsonBasedSchemaTransformer transformer = new JsonBasedSchemaTransformer(schemaMapping);

    ClusterSchemaDefinition transformedClusterSchema = transformer.transform(schemaDefinition);
    TableSchemaDefinition expectedTableSchema =
        new TableSchemaDefinition(
            new HTableDescriptor(
                TableName.valueOf("new-table1"),
                schemaDefinition.tableSchemaDefinitions.get(0).getHbaseTableDescriptor()),
            tableSchemaDefinition.splits);

    // Verify transformation
    Assert.assertEquals("new-table1", transformedClusterSchema.tableSchemaDefinitions.get(0).name);
    Assert.assertEquals(
        expectedTableSchema.getHbaseTableDescriptor(),
        transformedClusterSchema.tableSchemaDefinitions.get(0).getHbaseTableDescriptor());
    Assert.assertEquals(
        transformedClusterSchema.tableSchemaDefinitions.get(0).splits,
        schemaDefinition.tableSchemaDefinitions.get(0).splits);
  }
}
