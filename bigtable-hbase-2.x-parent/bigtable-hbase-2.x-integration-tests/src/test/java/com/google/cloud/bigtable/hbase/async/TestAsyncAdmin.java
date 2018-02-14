/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.async;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for BigtableAsyncAdmin
 * 
 * @author spollapally
 */
@RunWith(JUnit4.class)
public class TestAsyncAdmin extends AbstractAsyncTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAsyncConnection() throws Exception {
    Assert.assertNotNull("async connection should not be null", getAsyncConnection());
    AsyncAdmin asycAdmin = getAsyncConnection().getAdmin();
    Assert.assertNotNull("asycAdmin should not be null", asycAdmin);
  }

  @Test
  public void testCreateTable_exception() throws Exception {
    AsyncAdmin asyncAdmin = getAsyncConnection().getAdmin();
    thrown.expect(NullPointerException.class);
    asyncAdmin.createTable(null).get();
  }

  @Test
  public void testCreateTable_harness() throws Exception {
    AsyncAdmin asyncAdmin = getAsyncConnection().getAdmin();
    TableName tableName = sharedTestEnv.newTestTableName();

    try {
      // test exists
      assertEquals(false, asyncAdmin.tableExists(tableName).get());

      // test create new
      
      ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY);
      cfBuilder.setTimeToLive(10);
      
      asyncAdmin.createTable(TableDescriptorBuilder.newBuilder(tableName)
          .addColumnFamily(cfBuilder.build()).build()).get();
      assertEquals(true, asyncAdmin.tableExists(tableName).get());

      // test listTableNames all
      List<TableName> allTableNames = asyncAdmin.listTableNames().get();
      assertTrue("listTableNames-all should list atleast one table", allTableNames.size() > 0);
      assertTrue("listTableNames-all Should contain tableName" + tableName,
          allTableNames.stream().anyMatch(e -> tableName.equals(e)));

      // test listTableNames by pattern
      String tNStr = tableName.getNameAsString();
      List<TableName> patTableNames = asyncAdmin
          .listTableNames(Optional.of(Pattern.compile(tNStr.substring(0, 15).concat(".*"))), false)
          .get();
      assertTrue("listTableNames-pattern should list atleast one table", patTableNames.size() > 0);
      assertTrue("listTableNames-pattern should contain tableName" + tableName,
          patTableNames.stream().anyMatch(e -> tableName.equals(e)));

      // test listTables all
      List<TableDescriptor> allTableDescriptors = asyncAdmin.listTables().get();
      assertTrue("listTables-all should list atleast one table", allTableDescriptors.size() > 0);
      assertTrue("listTables-all should contain tableName" + tableName,
          allTableDescriptors.stream().anyMatch(e -> tableName.equals(e.getTableName())));

      // test listTables by pattern
      List<TableDescriptor> patTableDescriptors = asyncAdmin
          .listTables(Optional.of(Pattern.compile(tNStr.substring(0, 15).concat(".*"))), false)
          .get();
      assertTrue("listTables-pattern should list atleast one table",
          allTableDescriptors.size() > 0);
      assertTrue("listTables-pattern should contain tableName" + tableName,
          patTableDescriptors.stream().anyMatch(e -> tableName.equals(e.getTableName())));
      //TODO: Verify why this test fails. getColumnFamilies() array is empyty 
      //assertEquals(10, patTableDescriptors.get(0).getColumnFamilies()[0].getTimeToLive()); 

      // test isTableEnabled
      assertEquals(true, asyncAdmin.isTableEnabled(tableName).get());

      // test disableTable and isTableDisabled
      asyncAdmin.disableTable(tableName).get();
      assertEquals(true, asyncAdmin.isTableDisabled(tableName).get());

      // test deleteTable
      asyncAdmin.deleteTable(tableName).get();
      assertEquals(false, asyncAdmin.tableExists(tableName).get());
    } finally {
      // not strictly required. New test suite run for BigtableEnv will do cleanup by configured
      // table name patterns
      deleteTestTable(tableName);
    }
  }

  @Test
  public void testCreateTableWithNumRegions_exception() throws Exception {
    AsyncAdmin asyncAdmin = getAsyncConnection().getAdmin();
    TableName tableName = TableName.valueOf("TestTable" + UUID.randomUUID().toString());
    thrown.expect(ExecutionException.class);
    thrown.expectCause(IsInstanceOf.<Throwable>instanceOf(IllegalArgumentException.class));
    asyncAdmin.createTable(TableDescriptorBuilder.newBuilder(tableName).build(),
        Bytes.toBytes("AAA"), Bytes.toBytes("BBB"), 2).get();
  }

  @Test
  public void testCreateTableWithSplits() throws Exception {
    AsyncAdmin asyncAdmin = getAsyncConnection().getAdmin();
    TableName tableName1 = TableName.valueOf("TestTable" + UUID.randomUUID().toString());
    TableName tableName2 = TableName.valueOf("TestTable" + UUID.randomUUID().toString());

    try {
      asyncAdmin.createTable(
          TableDescriptorBuilder.newBuilder(tableName1)
              .addColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY)).build(),
          Bytes.toBytes("AAA"), Bytes.toBytes("BBB"), 3).get();
      assertEquals(true, asyncAdmin.tableExists(tableName1).get());

      byte[][] splitKeys =
          new byte[][] {Bytes.toBytes("AAA"), Bytes.toBytes("BBB"), Bytes.toBytes("CCC"),};

      asyncAdmin.createTable(
          TableDescriptorBuilder.newBuilder(tableName2)
              .addColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY)).build(),
          Optional.of(splitKeys)).get();
      assertEquals(true, asyncAdmin.tableExists(tableName2).get());

      // TODO - Add Region checks
    } finally {
      deleteTestTable(tableName1);
      deleteTestTable(tableName2);
    }
  }

  @Test
  public void testDisbleTable_exceptions() throws Exception {
    AsyncAdmin asyncAdmin = getAsyncConnection().getAdmin();
    TableName tableName = TableName.valueOf("TestTable" + UUID.randomUUID().toString());

    // test non existing table
    Throwable th = null;
    try {
      asyncAdmin.disableTable(tableName).get();
    } catch (ExecutionException e) {
      th = e;
    }
    assertThat(th.getCause(), IsInstanceOf.<Throwable>instanceOf(TableNotFoundException.class));

    // test already disabled table
    th = null;
    asyncAdmin.createTable(TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY)).build()).get();
    asyncAdmin.disableTable(tableName).get();
    try {
      asyncAdmin.disableTable(tableName).get();
    } catch (ExecutionException e) {
      th = e;
    }
    assertThat(th.getCause(), IsInstanceOf.<Throwable>instanceOf(TableNotEnabledException.class));
  }

  private void deleteTestTable(TableName tableName) {
    try {
      Admin admin = getConnection().getAdmin();
      if (admin.tableExists(tableName)) {
        getConnection().getAdmin().disableTable(tableName);
        getConnection().getAdmin().deleteTable(tableName);
      }
    } catch (Throwable t) {
      logger.warn("Error cleaning up the table", t);
    }
  }
}
