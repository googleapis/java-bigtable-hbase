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
import com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator.JsonBasedSchemaTransformer;
import com.google.common.collect.ImmutableMap;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
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
    schemaTransformer = new JsonBasedSchemaTransformer(schemaFilePath);
  }

  ///////////////////////////////// Invalid cases //////////////////////////////////////
  @Test
  public void testBadJson() throws IOException {

    // Setup bad JSON
    FileWriter fw = new FileWriter(schemaFilePath);
    fw.write("random JSON string");
    // Execute the test method
    try {
      schemaTransformer.parseMappingFile();
      fail("Expected IllegalStateException due to bad JSON.");
    } catch (IllegalStateException e) {
      e.printStackTrace();
    } catch (Exception e) {
      fail("Expected IllegalStateException due to bad JSON but found " + e.toString());
    }
  }

  //////////////////////////////////////// Happy cases ////////////////////////////////////
  @Test
  public void testParseJsonOnlyTables() throws IOException {
    // Setup the schema file
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("old-table", "new-table");
    // Make sure that the whole table name is matched and not just prefix
    jsonObject.addProperty("old-table-2", "random-table-2");
    try (Writer writer = new FileWriter(schemaFilePath)) {
      new Gson().toJson(jsonObject, writer);
    }

    // Parse schema file
    schemaTransformer.parseMappingFile();

    // Validate
    Map<String, String> expectedTableMapping =
        ImmutableMap.of("old-table", "new-table", "old-table-2", "random-table-2");
    assertEquals(expectedTableMapping, schemaTransformer.tableNameMappings);
    assertEquals(0, schemaTransformer.tableScopedColumnFamilyMappings.size());
  }

  @Test
  public void testParseJsonOnlyColumnFamily() throws IOException {
    // Setup the schema file
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("old-table:old-cf", "new-cf");
    jsonObject.addProperty("old-table:old-cf2", "random-cf2");
    jsonObject.addProperty("some-table:cf", "cf-new");
    try (Writer writer = new FileWriter(schemaFilePath)) {
      new Gson().toJson(jsonObject, writer);
    }

    // Parse schema file
    schemaTransformer.parseMappingFile();

    // Validate
    assertEquals(0, schemaTransformer.tableNameMappings.size());
    Map<String, Map<String, String>> expectedTableScopedCFMapping =
        ImmutableMap.of(
            "old-table", ImmutableMap.of("old-cf", "new-cf", "old-cf2", "random-cf2"),
            "some-table", ImmutableMap.of("cf", "cf-new"));
    assertEquals(expectedTableScopedCFMapping, schemaTransformer.tableScopedColumnFamilyMappings);
  }

  @Test
  public void testParseJsonEmpty() throws IOException {
    // Setup the schema file
    JsonObject jsonObject = new JsonObject();
    try (Writer writer = new FileWriter(schemaFilePath)) {
      new Gson().toJson(jsonObject, writer);
    }

    // Parse schema file
    schemaTransformer.parseMappingFile();
    assertEquals(0, schemaTransformer.tableNameMappings.size());
    assertEquals(0, schemaTransformer.tableScopedColumnFamilyMappings.size());
  }

  @Test
  public void testParseJsonTableAndColumnFamily() throws IOException {
    // Setup the schema file
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("old-table", "new-table");
    jsonObject.addProperty("old-table:old-cf", "new-cf");
    Gson gson = new Gson();
    try (Writer writer = new FileWriter(schemaFilePath)) {
      new Gson().toJson(jsonObject, writer);
    }

    schemaTransformer.parseMappingFile();
    Map<String, String> expectedTableMapping = Collections.singletonMap("old-table", "new-table");
    assertEquals(expectedTableMapping, schemaTransformer.tableNameMappings);
    Map<String, Map<String, String>> expectedTableScopedCFMapping =
        Collections.singletonMap("old-table", Collections.singletonMap("old-cf", "new-cf"));
    assertEquals(expectedTableScopedCFMapping, schemaTransformer.tableScopedColumnFamilyMappings);
  }
}
