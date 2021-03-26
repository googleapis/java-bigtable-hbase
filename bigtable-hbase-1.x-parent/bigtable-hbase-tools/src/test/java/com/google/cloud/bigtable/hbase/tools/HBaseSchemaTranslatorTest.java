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

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;

import com.google.cloud.bigtable.hbase.tools.ClusterSchemaDefinition.TableSchemaDefinition;
import com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator.BigtableBasedSchemaWriter;
import com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator.FileBasedSchemaReader;
import com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator.FileBasedSchemaWriter;
import com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator.HBaseSchemaReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class HBaseSchemaTranslatorTest {

  @Mock private Admin btAdmin;

  @Mock private Admin hbaseAdmin;

  @Rule public MockitoRule initRule = MockitoJUnit.rule();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private final ClusterSchemaDefinition schemaDefinition;

  public HBaseSchemaTranslatorTest() {
    schemaDefinition = new ClusterSchemaDefinition();
    TableSchemaDefinition tableSchemaDefinition = new TableSchemaDefinition();
    tableSchemaDefinition.name = "test-table1";
    tableSchemaDefinition.splits = new byte[2][];
    tableSchemaDefinition.splits[0] = "first-split".getBytes();
    tableSchemaDefinition.splits[1] = "second-split".getBytes();
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("test-table1"));
    HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
    columnDescriptor.setMaxVersions(2).setTimeToLive(1000);
    tableDescriptor.addFamily(columnDescriptor);
    tableSchemaDefinition.tableDescriptor = tableDescriptor.toByteArray();
    schemaDefinition.tableSchemaDefinitions.add(tableSchemaDefinition);

    TableSchemaDefinition tableSchemaDefinition2 = new TableSchemaDefinition();
    tableSchemaDefinition2.name = "test-table2";
    HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("test-table2"));
    HColumnDescriptor columnDescriptor2 = new HColumnDescriptor("cf");
    tableDescriptor2.addFamily(columnDescriptor2);
    tableSchemaDefinition2.splits = new byte[0][];
    tableSchemaDefinition2.tableDescriptor = tableDescriptor2.toByteArray();
    schemaDefinition.tableSchemaDefinitions.add(tableSchemaDefinition2);
  }

  public void showUsage() {}

  private List<HRegionInfo> getRegions(int tableIndex) {
    List<HRegionInfo> regions = new ArrayList<>();

    TableSchemaDefinition tableSchemaDefinition =
        schemaDefinition.tableSchemaDefinitions.get(tableIndex);

    if (tableSchemaDefinition.splits == null || tableSchemaDefinition.splits.length == 0) {
      return regions;
    }

    TableName tableName =
        TableName.valueOf(schemaDefinition.tableSchemaDefinitions.get(tableIndex).name);
    byte[] currentStart = HConstants.EMPTY_START_ROW;
    for (byte[] splitPoint : schemaDefinition.tableSchemaDefinitions.get(tableIndex).splits) {
      HRegionInfo regionInfo = new HRegionInfo(tableName, currentStart, splitPoint);
      regions.add(regionInfo);
      currentStart = splitPoint;
    }
    regions.add(new HRegionInfo(tableName, currentStart, HConstants.EMPTY_END_ROW));
    return regions;
  }

  private HTableDescriptor[] getTables() throws DeserializationException, IOException {
    HTableDescriptor[] tables =
        new HTableDescriptor[schemaDefinition.tableSchemaDefinitions.size()];
    for (int i = 0; i < schemaDefinition.tableSchemaDefinitions.size(); i++) {
      tables[i] = schemaDefinition.tableSchemaDefinitions.get(i).getHbaseTableDescriptor();
    }
    System.out.println("Created tables " + Arrays.asList(tables));
    return tables;
  }

  private void setUpHBaseAdmin() throws IOException, DeserializationException {

    // Setup expectations on mocks.
    Mockito.when(hbaseAdmin.listTables(eq(".*"))).thenReturn(getTables());
    Mockito.when(hbaseAdmin.getTableRegions(eq(TableName.valueOf("test-table1"))))
        .thenReturn(getRegions(0));
    Mockito.when(hbaseAdmin.getTableRegions(eq(TableName.valueOf("test-table2"))))
        .thenReturn(getRegions(1));
  }

  private void verifyMocks() throws DeserializationException, IOException {
    Mockito.verify(btAdmin)
        .createTable(
            eq(schemaDefinition.tableSchemaDefinitions.get(0).getHbaseTableDescriptor()),
            eq(schemaDefinition.tableSchemaDefinitions.get(0).splits));
    Mockito.verify(btAdmin)
        .createTable(
            eq(schemaDefinition.tableSchemaDefinitions.get(1).getHbaseTableDescriptor()),
            eq(schemaDefinition.tableSchemaDefinitions.get(1).splits));
    Mockito.verify(hbaseAdmin).listTables(".*");
    Mockito.verify(hbaseAdmin).getTableRegions(TableName.valueOf("test-table1"));
    Mockito.verify(hbaseAdmin).getTableRegions(TableName.valueOf("test-table2"));
  }

  @After
  public void tearDown() throws Exception {
    Mockito.validateMockitoUsage();
    Mockito.reset(btAdmin, hbaseAdmin);
  }

  ///////////////////////////////////// Happy Cases ///////////////////////////////////////////////

  // These tests only validates that the object received from HBase is passed to Bigtable client.
  // The actual translation is performed by Bigtable HBase client and hence not validated here.
  @Test
  public void testTranslateFromHBaseToBigtable() throws IOException, DeserializationException {
    // Setup HBase admin client.
    setUpHBaseAdmin();

    // Create a translator;
    HBaseSchemaTranslator translator =
        new HBaseSchemaTranslator(
            new HBaseSchemaReader(hbaseAdmin, ".*"), new BigtableBasedSchemaWriter(btAdmin));

    translator.translate();

    // Validate that everything was called properly.
    verifyMocks();
  }

  @Test
  public void testTranslateHBaseToBigtableViaFile() throws IOException, DeserializationException {
    // Setup HBase admin client.
    setUpHBaseAdmin();

    File schemaFile = tempFolder.newFile("schema.json");
    HBaseSchemaTranslator translator1 =
        new HBaseSchemaTranslator(
            new HBaseSchemaReader(hbaseAdmin, ".*"),
            new FileBasedSchemaWriter(schemaFile.getPath()));

    translator1.translate();

    HBaseSchemaTranslator translator2 =
        new HBaseSchemaTranslator(
            new FileBasedSchemaReader(schemaFile.getPath()),
            new BigtableBasedSchemaWriter(btAdmin));

    translator2.translate();

    // Validate that everything was called properly.
    verifyMocks();
  }

  ///////////////////////////////////// Exception Tests ///////////////////////////////////////////

  @Test
  public void testHBaseListTableFails() throws IOException {

    Mockito.when(hbaseAdmin.listTables(eq(".*"))).thenThrow(new IOException("List table failed"));

    // Create a translator;
    HBaseSchemaTranslator translator =
        new HBaseSchemaTranslator(
            new HBaseSchemaReader(hbaseAdmin, ".*"), new BigtableBasedSchemaWriter(btAdmin));
    try {
      translator.translate();
      fail("Expected IOException here.");
    } catch (IOException e) {
      // Expected.
      e.printStackTrace();
    } catch (Exception e) {
      fail("Expected IOException but found: " + e.toString());
    } finally {
      // Verify that the listTables was called.
      Mockito.verify(hbaseAdmin).listTables(".*");
    }
  }

  @Test
  public void testHBaseGetTableRegionFails() throws IOException, DeserializationException {

    Mockito.when(hbaseAdmin.listTables(eq(".*"))).thenReturn(getTables());
    Mockito.when(hbaseAdmin.getTableRegions(eq(TableName.valueOf("test-table1"))))
        .thenThrow(new IOException("getTableRegions failed."));

    // Create a translator;
    HBaseSchemaTranslator translator =
        new HBaseSchemaTranslator(
            new HBaseSchemaReader(hbaseAdmin, ".*"), new BigtableBasedSchemaWriter(btAdmin));
    try {
      translator.translate();
      fail("Expected IOException here.");
    } catch (IOException e) {
      // Expected.
      e.printStackTrace();
    } catch (Exception e) {
      fail("Expected IOException but found: " + e.toString());
    } finally {
      // Verify that the HBase admin was called.
      Mockito.verify(hbaseAdmin).listTables(".*");
      Mockito.verify(hbaseAdmin).getTableRegions(TableName.valueOf("test-table1"));
    }
  }

  @Test
  public void testBigtableCreateTableFails() throws IOException, DeserializationException {
    // Setup HBase to return the tables.
    setUpHBaseAdmin();

    Mockito.doThrow(new IOException("CBT create table failed."))
        .when(btAdmin)
        .createTable(
            eq(schemaDefinition.tableSchemaDefinitions.get(0).getHbaseTableDescriptor()),
            eq(schemaDefinition.tableSchemaDefinitions.get(0).splits));

    // Create a translator;
    HBaseSchemaTranslator translator =
        new HBaseSchemaTranslator(
            new HBaseSchemaReader(hbaseAdmin, ".*"), new BigtableBasedSchemaWriter(btAdmin));
    try {
      translator.translate();
      fail("Expected IOException here.");
    } catch (RuntimeException e) {
      // Expected.
      e.printStackTrace();
    } catch (Exception e) {
      fail("Expected IOException but found: " + e.toString());
    } finally {
      // Verify that the listTables was called.
      verifyMocks();
    }
  }
}
