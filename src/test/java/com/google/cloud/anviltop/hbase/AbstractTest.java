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

import org.apache.hadoop.conf.Configuration;
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

public abstract class AbstractTest {
  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected HConnection connection;
  protected static final TableName TABLE_NAME = TableName.valueOf("test_table");
  protected static final byte[] COLUMN_FAMILY = Bytes.toBytes("test_family");
  protected static int MAX_VERSIONS = 6;
  protected DataGenerationHelper dataHelper = new DataGenerationHelper();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    Admin admin = getAdmin();
    HColumnDescriptor hcd = new HColumnDescriptor(COLUMN_FAMILY).setMaxVersions(MAX_VERSIONS);
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    htd.addFamily(hcd);
    admin.createTable(htd);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Admin admin = getAdmin();
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  private static Admin getAdmin() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    HConnection connection = HConnectionManager.createConnection(conf);
    return connection.getAdmin();
  }

  @Before
  public void setUp() throws IOException {
    //Configuration conf = HBaseConfiguration.create();
    Configuration conf = TEST_UTIL.getConfiguration();
    this.connection = HConnectionManager.createConnection(conf);
    //Assert.assertTrue(this.connection instanceof AnvilTopConnection);
  }

  @After
  public void tearDown() throws IOException {
    this.connection.close();
  }

  // This is for when we need to look at the results outside of the current connection
  public HConnection createNewConnection() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
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
