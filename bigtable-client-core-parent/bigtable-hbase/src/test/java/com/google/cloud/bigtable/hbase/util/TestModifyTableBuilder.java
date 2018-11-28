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
package com.google.cloud.bigtable.hbase.util;

import static com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter.buildGarbageCollectionRule;

import com.google.bigtable.admin.v2.InstanceName;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestModifyTableBuilder {

  private static final String PROJECT_ID = "fakeProject";
  private static final String INSTANCE_ID = "fakeInstance";
  private static final String TABLE_ID = "myTable";
  private static final String COLUMN_FAMILY = "myColumnFamily";

  private InstanceName instanceName;
  @Before
  public void setUp() {
    instanceName = InstanceName.of(PROJECT_ID, INSTANCE_ID);
  }

  @Test
  public void testBuildModificationForAddFamily(){
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_ID));
    HColumnDescriptor addColumn = new HColumnDescriptor(COLUMN_FAMILY);
    tableDescriptor.addFamily(addColumn);
    ModifyTableBuilder actualRequest = ModifyTableBuilder
        .buildModifications(tableDescriptor, new HTableDescriptor(TableName.valueOf(TABLE_ID)));

    ModifyColumnFamiliesRequest expectedRequest = ModifyColumnFamiliesRequest.of(TABLE_ID)
        .addFamily(COLUMN_FAMILY, buildGarbageCollectionRule(addColumn));

    Assert.assertEquals(expectedRequest.toProto(instanceName),
        actualRequest.build().toProto(instanceName));
  }

  @Test
  public void testBuildModificationForUpdateFamily(){
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_ID));
    HColumnDescriptor addColumn = new HColumnDescriptor(COLUMN_FAMILY);
    tableDescriptor.addFamily(addColumn);
    ModifyTableBuilder actualRequest = ModifyTableBuilder
        .buildModifications(tableDescriptor, new HTableDescriptor(tableDescriptor));

    ModifyColumnFamiliesRequest expectedRequest = ModifyColumnFamiliesRequest.of(TABLE_ID)
        .updateFamily(COLUMN_FAMILY, buildGarbageCollectionRule(addColumn));

    Assert.assertEquals(expectedRequest.toProto(instanceName),
        actualRequest.build().toProto(instanceName));
  }

  @Test
  public void testBuildModificationForDropFamily(){
    final String NEW_COLUMN_FAMILY = "anotherColumnFamily";
    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_ID));
    HColumnDescriptor addColumn = new HColumnDescriptor(COLUMN_FAMILY);
    tableDescriptor.addFamily(addColumn);
    HTableDescriptor newTableDesc = new HTableDescriptor(TableName.valueOf(TABLE_ID));
    HColumnDescriptor newColumnDesc = new HColumnDescriptor(NEW_COLUMN_FAMILY);
    newTableDesc.addFamily(newColumnDesc);

    ModifyTableBuilder actualRequest =
        ModifyTableBuilder.buildModifications(tableDescriptor, newTableDesc);

    ModifyColumnFamiliesRequest expectedRequest = ModifyColumnFamiliesRequest.of(TABLE_ID)
        .addFamily(COLUMN_FAMILY, buildGarbageCollectionRule(addColumn))
        .dropFamily(NEW_COLUMN_FAMILY);

    Assert.assertEquals(expectedRequest.toProto(instanceName),
        actualRequest.build().toProto(instanceName));
  }
}
