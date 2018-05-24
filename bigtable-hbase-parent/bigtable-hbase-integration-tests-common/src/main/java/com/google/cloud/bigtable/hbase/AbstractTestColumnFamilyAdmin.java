/*
 * Copyright 2018 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import org.junit.rules.ExpectedException;

/**
 * Tests creation and deletion of column families.
 */
public abstract class AbstractTestColumnFamilyAdmin extends AbstractTest {
  protected static byte[] DELETE_COLUMN_FAMILY =
      Bytes.toBytes(Bytes.toString(COLUMN_FAMILY) + "_remove");

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  protected Admin admin;
  protected TableName tableName;

  @Before
  public void setup() throws Exception {
    admin = getConnection().getAdmin();
    tableName = sharedTestEnv.newTestTableName();
    sharedTestEnv.createTable(tableName);
  }

  @After
  public void tearDown() throws IOException {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  @Test
  public void testCreateTableFull() throws Exception {
    HTableDescriptor retrievedDescriptor = getTableDescriptor(tableName);
    assertEquals(2, retrievedDescriptor.getFamilies().size());
    assertNotNull(retrievedDescriptor.getFamily(COLUMN_FAMILY));
    assertNotNull(retrievedDescriptor.getFamily(COLUMN_FAMILY2));
    assertEquals(SharedTestEnvRule.MAX_VERSIONS, retrievedDescriptor.getFamily(COLUMN_FAMILY).getMaxVersions());
    assertEquals(SharedTestEnvRule.MAX_VERSIONS, retrievedDescriptor.getFamily(COLUMN_FAMILY2).getMaxVersions());
  }
  
  @Test
  public void testAddColumn() throws Exception {
    byte[] new_column = Bytes.toBytes("NEW_COLUMN");
    addColumn(new_column, 2);
    HTableDescriptor tblDesc = getTableDescriptor(tableName);
    assertNotNull(tblDesc.getFamily(new_column));
  }
  
  @Test
  public void testModifyColumnFamily() throws Exception {
    byte[] modifyColumn = Bytes.toBytes("MODIFY_COLUMN");
    addColumn(modifyColumn, 2);

    assertEquals(2, getTableDescriptor(tableName).getFamily(modifyColumn).getMaxVersions());
    modifyColumn(modifyColumn, 100);
    assertEquals(100, getTableDescriptor(tableName).getFamily(modifyColumn).getMaxVersions());
  }
  
  @Test
  public void testRemoveColumn() throws Exception {
    addColumn(DELETE_COLUMN_FAMILY, 2);
    HTableDescriptor descriptorBefore = getTableDescriptor(tableName);
    deleteColumn(DELETE_COLUMN_FAMILY);
    HTableDescriptor descriptorAfter = getTableDescriptor(tableName);
    assertEquals(descriptorBefore.getFamilies().size() - 1, descriptorAfter.getFamilies().size());
    assertNull(descriptorAfter.getFamily(DELETE_COLUMN_FAMILY));
  }

  @Test
  public void testGetOnNonExistantTable() throws Exception {
    expectedException.expect(TableNotFoundException.class);
    getTableDescriptor(TableName.valueOf("does-not-exist"));
  }

  protected abstract HTableDescriptor getTableDescriptor(TableName tableName) throws Exception;
  protected abstract void addColumn(byte[] columnName, int version) throws Exception;
  protected abstract void modifyColumn(byte[] columnName, int version) throws Exception;
  protected abstract void deleteColumn(byte[] columnName) throws Exception;
}
