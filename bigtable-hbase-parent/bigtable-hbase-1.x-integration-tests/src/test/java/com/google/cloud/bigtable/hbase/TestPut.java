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

import java.io.IOException;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

public class TestPut extends AbstractTestPut {

  @Test(expected = RetriesExhaustedWithDetailsException.class)
  @Category(KnownGap.class)
  public void testIOExceptionOnFailedPut() throws Exception {
    Table table = getDefaultTable();
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] badfamily = Bytes.toBytes("badcolumnfamily-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] qualifier = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] value = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(rowKey);
    put.addColumn(badfamily, qualifier, value);
    table.put(put);
  }

  @Test
  @Category(KnownGap.class)
  public void testAtomicPut() throws Exception {
    Table table = getDefaultTable();
    byte[] rowKey = Bytes.toBytes("testrow-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] goodQual = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] goodValue = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] badQual = Bytes.toBytes("testQualifier-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] badValue = Bytes.toBytes("testValue-" + RandomStringUtils.randomAlphanumeric(8));
    byte[] badfamily = Bytes.toBytes("badcolumnfamily-" + RandomStringUtils.randomAlphanumeric(8));
    Put put = new Put(rowKey);
    put.addColumn(COLUMN_FAMILY, goodQual, goodValue);
    put.addColumn(badfamily, badQual, badValue);
    RetriesExhaustedWithDetailsException thrownException = null;
    try {
      table.put(put);
    } catch (RetriesExhaustedWithDetailsException e) {
      thrownException = e;
    }
    Assert.assertNotNull("Exception should have been thrown", thrownException);
    Assert.assertEquals("Expecting one exception", 1, thrownException.getNumExceptions());
    Assert.assertArrayEquals("Row key", rowKey, thrownException.getRow(0).getRow());
    Assert.assertTrue("Cause: NoSuchColumnFamilyException",
        thrownException.getCause(0) instanceof NoSuchColumnFamilyException);

    Get get = new Get(rowKey);
    Result result = table.get(get);
    Assert.assertEquals("Atomic behavior means there should be nothing here", 0, result.size());
    table.close();
  }

  protected Get getGetAddColumnVersion(int version, byte[] rowKey, byte[] qualifier) throws IOException {
    Get get = new Get(rowKey);
    get.addColumn(SharedTestEnvRule.COLUMN_FAMILY, qualifier);
    get.setMaxVersions(version);
    return get;
  }
}
