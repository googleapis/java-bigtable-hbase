/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.List;

@SuppressWarnings("deprecation")
public class TestCreateTable extends AbstractTestCreateTable {

  @Override
  protected void createTable(TableName tableName) throws IOException {
    getConnection().getAdmin().createTable(createDescriptor(tableName));
  }

  @Override
  protected void createTable(TableName tableName, byte[] start, byte[] end,
      int splitCount)
      throws IOException {
    getConnection().getAdmin().createTable(createDescriptor(tableName), start, end, splitCount);
  }

  @Override
  protected void createTable(TableName tableName, byte[][] ranges) throws IOException {
    getConnection().getAdmin().createTable(createDescriptor(tableName), ranges);
  }

  private HTableDescriptor createDescriptor(TableName tableName) {
    return new HTableDescriptor(tableName)
        .addFamily(new HColumnDescriptor(COLUMN_FAMILY));
  }

  @Override
  protected List<HRegionLocation> getRegions(TableName tableName)
      throws Exception {
    return getConnection().getRegionLocator(tableName).getAllRegionLocations();
  }

}
