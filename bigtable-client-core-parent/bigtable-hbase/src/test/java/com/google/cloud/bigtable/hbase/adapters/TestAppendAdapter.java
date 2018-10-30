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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRule;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class TestAppendAdapter {
  private static final String PROJECT_ID = "test-project-id";
  private static final String INSTANCE_ID = "test-instance-id";
  private static final String TABLE_ID = "test-table-id";
  public static final String APP_PROFILE_ID = "test-app-profile-id";
  protected AppendAdapter appendAdapter = new AppendAdapter();
  protected DataGenerationHelper dataHelper = new DataGenerationHelper();
  @Mock
  private RequestContext requestContext;
  @Mock
  private InstanceName instanceName;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    Mockito.when(instanceName.getProject()).thenReturn(PROJECT_ID);
    Mockito.when(instanceName.getInstance()).thenReturn(INSTANCE_ID);
    Mockito.when(requestContext.getInstanceName()).thenReturn(instanceName);
    Mockito.when(requestContext.getAppProfileId()).thenReturn(APP_PROFILE_ID);
  }

  @Test
  public void testBasicRowKeyAppend() {
    byte[] rowKey = dataHelper.randomData("rk1-");
    Append append = new Append(rowKey);
    ReadModifyWriteRow readModifyWriteRow = ReadModifyWriteRow.create(TABLE_ID, ByteString.copyFrom(rowKey));
    appendAdapter.adapt(append, readModifyWriteRow);
    ReadModifyWriteRowRequest request = readModifyWriteRow.toProto(requestContext);
    ByteString adaptedRowKey = request.getRowKey();
    Assert.assertArrayEquals(rowKey, adaptedRowKey.toByteArray());
  }

  @Test
  public void testMultipleAppends() {
    byte[] rowKey = dataHelper.randomData("rk1-");

    byte[] family1 = Bytes.toBytes("family1");
    byte[] qualifier1 = Bytes.toBytes("qualifier1");
    byte[] value1 = Bytes.toBytes("value1");

    byte[] family2 = Bytes.toBytes("family2");
    byte[] qualifier2 = Bytes.toBytes("qualifier2");
    byte[] value2 = Bytes.toBytes("value2");

    Append append = new Append(rowKey);
    append.add(family1, qualifier1, value1);
    append.add(family2, qualifier2, value2);

    ReadModifyWriteRow readModifyWriteRow = ReadModifyWriteRow.create(TABLE_ID, ByteString.copyFrom(rowKey));
    appendAdapter.adapt(append, readModifyWriteRow);
    ReadModifyWriteRowRequest request = readModifyWriteRow.toProto(requestContext);
    List<ReadModifyWriteRule> rules = request.getRulesList();
    Assert.assertEquals(2, rules.size());

    Assert.assertEquals("family1", rules.get(0).getFamilyName());
    Assert.assertEquals("qualifier1", rules.get(0).getColumnQualifier().toStringUtf8());
    Assert.assertEquals("value1", rules.get(0).getAppendValue().toStringUtf8());

    Assert.assertEquals("family2", rules.get(1).getFamilyName());
    Assert.assertEquals("qualifier2", rules.get(1).getColumnQualifier().toStringUtf8());
    Assert.assertEquals("value2", rules.get(1).getAppendValue().toStringUtf8());
  }

  @Test
  public void testMultipleAppendsWithDuplicates() {
    byte[] rowKey = dataHelper.randomData("rk1-");

    byte[] family1 = Bytes.toBytes("family1");
    byte[] qualifier1 = Bytes.toBytes("qualifier1");
    byte[] value1 = Bytes.toBytes("value1");

    byte[] family2 = Bytes.toBytes("family2");
    byte[] qualifier2 = Bytes.toBytes("qualifier2");
    byte[] value2 = Bytes.toBytes("value2");

    byte[] value3 = Bytes.toBytes("value3");

    Append append = new Append(rowKey);
    append.add(family1, qualifier1, value1);
    append.add(family2, qualifier2, value2);
    append.add(family2, qualifier2, value3);

    ReadModifyWriteRow readModifyWriteRow = ReadModifyWriteRow.create(TABLE_ID, ByteString.copyFrom(rowKey));
    appendAdapter.adapt(append, readModifyWriteRow);
    ReadModifyWriteRowRequest request = readModifyWriteRow.toProto(requestContext);
    List<ReadModifyWriteRule> rules = request.getRulesList();
    Assert.assertEquals(2, rules.size());

    Assert.assertEquals("family1", rules.get(0).getFamilyName());
    Assert.assertEquals("qualifier1", rules.get(0).getColumnQualifier().toStringUtf8());
    Assert.assertEquals("value1", rules.get(0).getAppendValue().toStringUtf8());

    Assert.assertEquals("family2", rules.get(1).getFamilyName());
    Assert.assertEquals("qualifier2", rules.get(1).getColumnQualifier().toStringUtf8());
    // Value3 as it was added after value2:
    Assert.assertEquals("value3", rules.get(1).getAppendValue().toStringUtf8());
  }

  @Test
  public void testMultipleColumnFamiliesWithSameQualifiers() {
    byte[] rowKey = dataHelper.randomData("rk1-");

    byte[] family1 = Bytes.toBytes("family1");
    byte[] qualifier1 = Bytes.toBytes("qualifier1");
    byte[] value1 = Bytes.toBytes("value1");

    byte[] family2 = Bytes.toBytes("family2");
    byte[] value2 = Bytes.toBytes("value2");

    Append append = new Append(rowKey);
    append.add(family1, qualifier1, value1);
    append.add(family2, qualifier1, value2);

    ReadModifyWriteRow readModifyWriteRow = ReadModifyWriteRow.create(TABLE_ID, ByteString.copyFrom(rowKey));
    appendAdapter.adapt(append, readModifyWriteRow);
    ReadModifyWriteRowRequest request = readModifyWriteRow.toProto(requestContext);
    List<ReadModifyWriteRule> rules = request.getRulesList();
    Assert.assertEquals(2, rules.size());

    Assert.assertEquals("family1", rules.get(0).getFamilyName());
    Assert.assertEquals("qualifier1", rules.get(0).getColumnQualifier().toStringUtf8());
    Assert.assertEquals("value1", rules.get(0).getAppendValue().toStringUtf8());

    Assert.assertEquals("family2", rules.get(1).getFamilyName());
    Assert.assertEquals("qualifier1", rules.get(1).getColumnQualifier().toStringUtf8());
    // Value3 as it was added after value2:
    Assert.assertEquals("value2", rules.get(1).getAppendValue().toStringUtf8());
  }
}
