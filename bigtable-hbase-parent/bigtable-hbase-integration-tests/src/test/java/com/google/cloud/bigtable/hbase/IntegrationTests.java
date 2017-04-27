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

import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.google.common.base.Preconditions;
import org.slf4j.bridge.SLF4JBridgeHandler;

@SuppressWarnings("deprecation")
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestAppend.class,
    TestBasicOps.class,
    TestBatch.class,
    TestBufferedMutator.class,
    TestCheckAndMutate.class,
    TestColumnFamilyAdmin.class,
    TestCreateTable.class,
    TestDisableTable.class,
    TestDelete.class,
    TestDurability.class,
    TestFilters.class,
    TestSingleColumnValueFilter.class,
    TestGet.class,
    TestGetTable.class,
    TestScan.class,
    TestSnapshots.class,
    TestIncrement.class,
    TestListTables.class,
    TestPut.class,
    TestTimestamp.class,
    TestTruncateTable.class,
    TestImport.class
})
public class IntegrationTests {
  @ClassRule
  public static Timeout timeoutRule = new Timeout(10, TimeUnit.MINUTES);

  @ClassRule
  public static SharedTestEnvRule sharedTestEnvRule = new SharedTestEnvRule();
}
