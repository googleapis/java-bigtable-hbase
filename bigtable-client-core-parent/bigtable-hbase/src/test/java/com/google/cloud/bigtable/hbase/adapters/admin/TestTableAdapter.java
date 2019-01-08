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
package com.google.cloud.bigtable.hbase.adapters.admin;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.Table;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules.GCRule;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestTableAdapter {

  private static final String PROJECT_ID = "fakeProject";
  private static final String INSTANCE_ID = "fakeInstance";
  private static final String TABLE_ID = "myTable";
  private static final String INSTANCE_NAME = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID;
  private static final String TABLE_NAME = INSTANCE_NAME + "/tables/" + TABLE_ID;
  private static final String COLUMN_FAMILY = "myColumnFamily";

  private TableAdapter tableAdapter;

  @Before
  public void setUp(){
    BigtableInstanceName bigtableInstanceName = new BigtableInstanceName(PROJECT_ID, INSTANCE_ID);
    tableAdapter = new TableAdapter(bigtableInstanceName);
  }

  @Test
  public void testAdaptWithHTableDescriptor(){
    byte[][] splits = new byte[][] {
            Bytes.toBytes("AAA"),
            Bytes.toBytes("BBB"),
            Bytes.toBytes("CCC"),
    };
    CreateTableRequest actualRequest =
            TableAdapter.adapt(new HTableDescriptor(TableName.valueOf(TABLE_ID)), splits);

    CreateTableRequest expectedRequest = CreateTableRequest.of(TABLE_ID);
    TableAdapter.addSplitKeys(splits, expectedRequest);
    Assert.assertEquals(
            expectedRequest.toProto(PROJECT_ID, INSTANCE_ID),
            actualRequest.toProto(PROJECT_ID, INSTANCE_ID));
  }


  @Test
  public void testAdaptWithHTableDescriptorWhenSplitIsEmpty(){
    byte[][] splits = new byte[0][0];
    CreateTableRequest actualRequest =
            TableAdapter.adapt(new HTableDescriptor(TableName.valueOf(TABLE_ID)), splits);

    CreateTableRequest expectedRequest = CreateTableRequest.of(TABLE_ID);
    Assert.assertEquals(
            expectedRequest.toProto(PROJECT_ID, INSTANCE_ID),
            actualRequest.toProto(PROJECT_ID, INSTANCE_ID));
  }

  @Test
  public void testAdaptWithColumnDesc(){
    HColumnDescriptor columnDesc = new HColumnDescriptor(COLUMN_FAMILY);
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_ID));
    CreateTableRequest request = CreateTableRequest.of(TABLE_ID);
    desc.addFamily(columnDesc);

    TableAdapter.adapt(desc, request);

    GCRule gcRule = ColumnDescriptorAdapter.buildGarbageCollectionRule(columnDesc);
    CreateTableRequest expected =
            CreateTableRequest.of(TABLE_ID).addFamily(COLUMN_FAMILY, gcRule);
    Assert.assertEquals(request.toProto(PROJECT_ID, INSTANCE_ID), expected.toProto(PROJECT_ID, INSTANCE_ID));
  }


  @Test
  public void testAdaptForTable(){
    //If no GcRule passed to ColumnFamily, then ColumnDescriptorAdapter#buildGarbageCollectionRule
    //updates maxVersion to Integer.MAX_VALUE
    GCRule gcRule = GCRULES.maxVersions(1);
    ColumnFamily columnFamily = ColumnFamily.newBuilder()
            .setGcRule(gcRule.toProto()).build();
    Table table = Table.newBuilder()
            .setName(TABLE_NAME)
            .putColumnFamilies(COLUMN_FAMILY, columnFamily).build();
    HTableDescriptor actualTableDesc = tableAdapter.adapt(table);

    HTableDescriptor expected = new HTableDescriptor(TableName.valueOf(TABLE_ID));
    expected.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
    Assert.assertEquals(expected, actualTableDesc);
  }
}
