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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestTruncateTable extends AbstractTestTruncateTable {

  @Override
  protected void createTable(TableName tableName, byte[][] splits) throws IOException {
    try(Admin admin = getConnection().getAdmin()) {
      HTableDescriptor descriptor = new HTableDescriptor(tableName);
      descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
      admin.createTable(descriptor, splits);
    }
  }

	@Override
	protected void doTruncate(TableName tableName) throws Exception {
		TableName newTestTableName = sharedTestEnv.newTestTableName();
    sharedTestEnv.createTable(newTestTableName);
		try(Admin admin = getConnection().getAdmin()) {
    	admin.truncateTableAsync(newTestTableName, true).get();	
    }
	}
}
