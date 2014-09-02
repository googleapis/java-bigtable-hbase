/*
 * Copyright (c) 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase;

import com.google.api.client.util.Strings;
import com.google.api.client.util.Throwables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

public abstract class AbstractTest {
  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected HConnection connection;
  protected static final TableName TABLE_NAME;
  protected static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");
  protected static int MAX_VERSIONS = 6;
  protected DataGenerationHelper dataHelper = new DataGenerationHelper();
  protected static final Configuration BASE_CONFIGURATION = HBaseConfiguration.create();
  static {
    TABLE_NAME = TableName.valueOf("test_table-" + UUID.randomUUID().toString());
    addExtraResources(BASE_CONFIGURATION);
  }

  protected static void addExtraResources(Configuration configuration) {
    String extraResources = System.getProperty("anviltop.test.extra.resources");
    if (extraResources != null) {
      InputStream resourceStream =
          AbstractTest.class.getClassLoader().getResourceAsStream(extraResources);
      if (resourceStream != null) {
        configuration.addResource(resourceStream);
      }
    }
  }

  protected static boolean useMiniCluster() {
    return Strings.isNullOrEmpty(
        BASE_CONFIGURATION.get(HConnection.HBASE_CLIENT_CONNECTION_IMPL, ""));
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    if (useMiniCluster()) {
      TEST_UTIL.startMiniCluster(1);
    }
    Admin admin = getAdmin();
    HColumnDescriptor hcd = new HColumnDescriptor(COLUMN_FAMILY).setMaxVersions(MAX_VERSIONS);
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    if (useMiniCluster()) {
      htd.addFamily(hcd);
      admin.createTable(htd);
    } else {
      // TODO: Remove once create table with column families is present
      admin.createTable(htd);
      admin.addColumn(htd.getTableName(), hcd);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Admin admin = getAdmin();
    if (useMiniCluster()) {
      // TODO: Remove when disableTable is present
      admin.disableTable(TABLE_NAME);
    }
    admin.deleteTable(TABLE_NAME);
    if (useMiniCluster()) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  private static Configuration getConfiguration() {
    if (useMiniCluster()) {
      return TEST_UTIL.getConfiguration();
    } else {
      return BASE_CONFIGURATION;
    }
  }


  private static Admin getAdmin() throws IOException {
    Configuration conf = getConfiguration();
    HConnection connection = HConnectionManager.createConnection(conf);
    return connection.getAdmin();
  }

  @Before
  public void setUp() throws IOException {
    //Configuration conf = HBaseConfiguration.create();
    Configuration conf = getConfiguration();
    this.connection = HConnectionManager.createConnection(conf);
    //Assert.assertTrue(this.connection instanceof AnvilTopConnection);
  }

  @After
  public void tearDown() throws IOException {
    this.connection.close();
  }

  // This is for when we need to look at the results outside of the current connection
  public HConnection createNewConnection() throws IOException {
    Configuration conf = getConfiguration();
    HConnection newConnection = HConnectionManager.createConnection(conf);
    return newConnection;
  }

  protected static class QualifierValue implements Comparable<QualifierValue> {
    protected final byte[] qualifier;
    protected final byte[] value;

    public QualifierValue(@NotNull byte[] qualifier, @NotNull byte[] value) {
      this.qualifier = qualifier;
      this.value = value;
    }

    @Override
    public int compareTo(QualifierValue qualifierValue) {
      return Bytes.compareTo(this.qualifier, qualifierValue.qualifier);
    }
  }
}
