/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.com.google.cloud.bigtable.hbase2_x.adapters.admin;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.InstanceName;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.adapters.admin.TableAdapter;
import com.google.cloud.bigtable.hbase2_x.adapters.admin.TableAdapter2x;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestTableAdapter2x {

  private static final String PROJECT_ID = "fakeProject";
  private static final String INSTANCE_ID = "fakeInstance";
  private static final String TABLE_ID = "myTable";
  private static final String INSTANCE_NAME = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID;
  private static final String TABLE_NAME = INSTANCE_NAME + "/tables/" + TABLE_ID;
  private static final String COLUMN_FAMILY = "myColumnFamily";

  private TableAdapter2x tableAdapter2x;
  private InstanceName instanceName;

  @Before
  public void setUp(){
    BigtableOptions bigtableOptions = BigtableOptions.builder().setProjectId(PROJECT_ID)
            .setInstanceId(INSTANCE_ID).build();
    tableAdapter2x = new TableAdapter2x(bigtableOptions);
    instanceName = InstanceName.of(PROJECT_ID, INSTANCE_ID);
  }

  @Test
  public void testAdaptWithSplitKeys(){
    byte[][] splits = new byte[][] {
            Bytes.toBytes("AAA"),
            Bytes.toBytes("BBB"),
            Bytes.toBytes("CCC"),
    };
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_ID)).build();
    CreateTableRequest actualRequest = TableAdapter2x.adapt(desc, splits);

    CreateTableRequest expectedRequest = CreateTableRequest.of(TABLE_ID);
    TableAdapter.addSplitKeys(splits, expectedRequest);
    Assert.assertEquals(
            expectedRequest.toProto(instanceName),
            actualRequest.toProto(instanceName));
  }

  @Test
  public void testAdaptWithTable(){
    //If no GcRule passed to ColumnFamily, then ColumnDescriptorAdapter#buildGarbageCollectionRule
    //updates maxVersion to Integer.MAX_VALUE
    int maxVersion = 1;
    GCRules.GCRule gcRule = GCRULES.maxVersions(maxVersion);
    ColumnFamily columnFamily = ColumnFamily.newBuilder()
            .setGcRule(gcRule.toProto()).build();
    Table table = Table.newBuilder()
            .setName(TABLE_NAME)
            .putColumnFamilies(COLUMN_FAMILY, columnFamily).build();
    TableDescriptor actualTableDesc = tableAdapter2x.adapt(table);

    TableDescriptor expected = new HTableDescriptor(TableName.valueOf(TABLE_ID))
            .addFamily(new HColumnDescriptor(COLUMN_FAMILY));

    Assert.assertEquals(expected, actualTableDesc);
  }

  @Test
  public void testHColumnDescriptorAdapter(){
    ColumnFamilyDescriptor columnFamilyDesc = ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY);
    HColumnDescriptor actualDesc = TableAdapter2x.toHColumnDescriptor(columnFamilyDesc);
    HColumnDescriptor columnDes = new HColumnDescriptor(COLUMN_FAMILY);

    Assert.assertEquals(columnDes, actualDesc);
  }
}
