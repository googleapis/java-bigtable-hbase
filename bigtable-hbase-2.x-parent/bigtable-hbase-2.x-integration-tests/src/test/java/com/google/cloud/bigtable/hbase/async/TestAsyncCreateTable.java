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
package com.google.cloud.bigtable.hbase.async;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;

import com.google.cloud.bigtable.hbase.AbstractTestCreateTable;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

public class TestAsyncCreateTable extends AbstractTestCreateTable {

  protected static DataGenerationHelper dataHelper = new DataGenerationHelper();

  @Override
  protected void createTable(TableName tableName) throws Exception {
    try {
      getAsyncAdmin().createTable(createDescriptor(tableName)).get();
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected void createTable(TableName tableName, byte[] start, byte[] end, int splitCount)
      throws Exception {
    try {
      getAsyncAdmin().createTable(createDescriptor(tableName), start, end, splitCount).get();
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected void createTable(TableName tableName, byte[][] ranges) throws Exception {
    try {
      getAsyncAdmin().createTable(createDescriptor(tableName), ranges).get();
    } catch (ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  private TableDescriptor createDescriptor(TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build())
        .build();
  }

  private AsyncAdmin getAsyncAdmin() throws InterruptedException, ExecutionException {
    return AbstractAsyncTest.getAsyncConnection().getAdmin();
  }

  @Override
  protected List<HRegionLocation> getRegions(TableName tableName) throws Exception {
    byte[] rowKey = dataHelper.randomData("TestAsyncCreateTable-");
    List<HRegionLocation> regionLocationList = new ArrayList<>();
    HRegionLocation hRegionLocation =
        AbstractAsyncTest.getAsyncConnection()
            .getRegionLocator(tableName)
            .getRegionLocation(rowKey, true)
            .get();
    regionLocationList.add(hRegionLocation);
    return regionLocationList;
  }

  @Override
  protected boolean asyncGetRegions(TableName tableName) throws Exception {
    return getAsyncAdmin().getRegions(tableName).get().size() == 1 ? true : false;
  }

  @Override
  protected boolean isTableEnabled(TableName tableName) throws Exception {
    return getAsyncAdmin().isTableEnabled(tableName).get();
  }

  @Override
  protected void disableTable(TableName tableName) throws Exception {
    getAsyncAdmin().disableTable(tableName).get();
  }

  @Override
  protected void adminDeleteTable(TableName tableName) throws Exception {
    getAsyncAdmin().deleteTable(tableName).get();
  }

  @Override
  protected boolean tableExists(TableName tableName) throws Exception {
    return getAsyncAdmin().tableExists(tableName).get();
  }
}
