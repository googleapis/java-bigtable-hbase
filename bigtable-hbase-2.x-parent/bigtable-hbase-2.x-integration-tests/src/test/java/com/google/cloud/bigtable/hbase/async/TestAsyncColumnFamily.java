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
package com.google.cloud.bigtable.hbase.async;

import static com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule.COLUMN_FAMILY;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.bigtable.hbase.AbstractTestColumnFamilyAdmin;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;

public class TestAsyncColumnFamily extends AbstractTestColumnFamilyAdmin {
  private AsyncAdmin asyncAdmin = null;

  public TestAsyncColumnFamily () {
    try {
      asyncAdmin =  AbstractAsyncTest.getAsyncConnection().getAdmin();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected HTableDescriptor getTableDescriptor(TableName tableName) throws Exception {
    try {
      return new HTableDescriptor(asyncAdmin.getDescriptor(tableName).get());
    } catch(ExecutionException e) {
      throw (Exception) e.getCause();
    }
  }

  @Override
  protected void addColumn(byte[] columnName, int version) throws Exception {
    ColumnFamilyDescriptorBuilder colFamilyDescBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(columnName);
    colFamilyDescBuilder.setMaxVersions(version);
    asyncAdmin.addColumnFamily(tableName, colFamilyDescBuilder.build()).get();
  }
  
  @Override
  protected void modifyColumn(byte[] columnName, int version) throws Exception {
    ColumnFamilyDescriptorBuilder colFamilyDescBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(columnName);
    colFamilyDescBuilder.setMaxVersions(version);
    asyncAdmin.modifyColumnFamily(tableName, colFamilyDescBuilder.build()).get();
  }

  @Override
  protected void deleteColumn(byte[] columnName) throws Exception {
    asyncAdmin.deleteColumnFamily(tableName, columnName).get();
  }
  
}
