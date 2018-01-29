/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.test_env;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.rules.ExternalResource;

public class SharedTestEnvRule extends ExternalResource {

  public static final int MAX_VERSIONS = 6;
  public static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");
  public static final byte[] COLUMN_FAMILY2 = Bytes.toBytes("test_family2");
  private static final Log LOG = LogFactory.getLog(SharedTestEnvRule.class);
  private TableName defaultTableName;
  private SharedTestEnv sharedTestEnv;
  private Connection connection;
  private AsyncConnection asyncConnection;

  @Override
  protected void before() throws Throwable {

    sharedTestEnv = SharedTestEnv.get();
    connection = createConnection();
    asyncConnection = createAsyncConnection();

    defaultTableName = newTestTableName();
    createTable(defaultTableName);
  }

  @Override
  protected void after() {
    try (Admin admin = connection.getAdmin()) {
      LOG.info("Deleting table " + defaultTableName.getNameAsString());
      admin.disableTable(defaultTableName);
      admin.deleteTable(defaultTableName);
    } catch (Exception e) {
      throw new RuntimeException("Error deleting table after the integration tests", e);
    }

    try {
      connection.close();
    } catch (IOException e) {
      LOG.error("Failed to close connection after test", e);
    }
    connection = null;

    try {
      asyncConnection.close();
    } catch (Exception e) {
      LOG.error("Failed to close asyncConnection after test", e);
    }
    asyncConnection = null;
    
    try {
      sharedTestEnv.release();
    } catch (IOException e) {
      LOG.error("Failed to release the environment after test", e);
    }
    sharedTestEnv = null;
  }

  public Connection getConnection() {
    return connection;
  }

  public Connection createConnection() throws IOException {
    return sharedTestEnv.createConnection();
  }

  public AsyncConnection createAsyncConnection() throws Exception {
    return sharedTestEnv.createAsyncConnection();
  }

  public AsyncConnection getAsynConnection() {
    return asyncConnection;
  }

  public Table getDefaultTable() throws IOException {
    return getConnection().getTable(defaultTableName);
  }

  public boolean isBigtable() {
    // TODO(igorbernstein2): clean this up
    return sharedTestEnv instanceof BigtableEnv;
  }

  public TableName getDefaultTableName() {
    return defaultTableName;
  }

  public TableName newTestTableName() {
    return TableName.valueOf("test_table-" + UUID.randomUUID().toString());
  }

  public void createTable(TableName tableName) throws IOException {
    try (Admin admin = connection.getAdmin();) {
//      ColumnFamilyDescriptor hcd = ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY)
//          .setMaxVersions(MAX_VERSIONS).build();
//      ColumnFamilyDescriptor hcdfamily2 = ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY2)
//          .setMaxVersions(MAX_VERSIONS).build();
//      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
//          .addColumnFamily(hcd).addColumnFamily(hcdfamily2).build();
//
//      admin.createTable(tableDescriptor);
      LOG.info("Creating table " + defaultTableName.getNameAsString());
      HColumnDescriptor hcd = new HColumnDescriptor(COLUMN_FAMILY).setMaxVersions(MAX_VERSIONS);
      HColumnDescriptor family2 = new HColumnDescriptor(COLUMN_FAMILY2).setMaxVersions(MAX_VERSIONS);
      admin.createTable(
          new HTableDescriptor(tableName)
              .addFamily(hcd)
              .addFamily(family2));

    }
  }
}
