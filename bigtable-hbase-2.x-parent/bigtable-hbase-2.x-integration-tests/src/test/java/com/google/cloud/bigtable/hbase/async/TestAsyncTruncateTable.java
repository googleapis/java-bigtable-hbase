/*
 * Copyright 2018 Google LLC All Rights Reserved.
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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.hbase.AbstractTestTruncateTable;

@RunWith(JUnit4.class)
public class TestAsyncTruncateTable extends AbstractTestTruncateTable {

	private AsyncAdmin getAsyncAdmin() throws InterruptedException, ExecutionException {
    return AbstractAsyncTest.getAsyncConnection().getAdmin();
  }
	
  private TableDescriptor createDescriptor(TableName tableName) {
    return TableDescriptorBuilder.newBuilder(tableName)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build())
        .build();
  }

	@Override
	protected void createTable(TableName tableName, byte[][] ranges) throws IOException {
		try{
			getAsyncAdmin().createTable(createDescriptor(tableName), ranges).get();
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
  }

	@Override
	protected void doTruncate(TableName tableName) throws Exception {
		getAsyncAdmin().createTable(createDescriptor(tableName)).get();
		getAsyncAdmin().truncateTable(tableName, true).get();
	}
}
