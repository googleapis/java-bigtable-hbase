/*
 * Copyright 2018 Google LLC
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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestCreateTableHBase2 extends AbstractTestCreateTable {

  @Test
  public void testCreateAsync() throws Exception {
    TableName tableName = sharedTestEnv.newTestTableName();
    createTableAsync(tableName);
    Assert.assertTrue(tableExists(tableName));
    deleteTable(tableName);
    Assert.assertFalse(tableExists(tableName));
  }

  private void createTableAsync(TableName tableName) throws Exception {
    getConnection().getAdmin().createTableAsync(createDescriptor(tableName)).get();
  }

  @Override
  protected void createTable(TableName tableName) throws IOException {
    getConnection().getAdmin().createTable(createDescriptor(tableName));
  }

  @Override
  protected void createTable(TableName tableName, byte[] start, byte[] end, int splitCount)
      throws IOException {
    getConnection().getAdmin().createTable(createDescriptor(tableName), start, end, splitCount);
  }

  @Override
  protected void createTable(TableName tableName, byte[][] ranges) throws IOException {
    getConnection().getAdmin().createTable(createDescriptor(tableName), ranges);
  }

  private TableDescriptor createDescriptor(TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build())
        .build();
  }

  @Override
  protected List<HRegionLocation> getRegions(TableName tableName) throws Exception {
    return getConnection().getRegionLocator(tableName).getAllRegionLocations();
  }

  @Override
  protected boolean asyncGetRegions(TableName tableName) throws Exception {
    return getConnection().getAdmin().getRegions(tableName).size() == 1 ? true : false;
  }

  @Override
  protected boolean isTableEnabled(TableName tableName) throws Exception {
    return getConnection().getAdmin().isTableEnabled(tableName);
  }

  @Override
  protected void disableTable(TableName tableName) throws Exception {
    getConnection().getAdmin().disableTable(tableName);
  }

  @Override
  protected void adminDeleteTable(TableName tableName) throws Exception {
    getConnection().getAdmin().deleteTable(tableName);
  }

  @Override
  protected boolean tableExists(TableName tableName) throws Exception {
    return getConnection().getAdmin().tableExists(tableName);
  }
}
