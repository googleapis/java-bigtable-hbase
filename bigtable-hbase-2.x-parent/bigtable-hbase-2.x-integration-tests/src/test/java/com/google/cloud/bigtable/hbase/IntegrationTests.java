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

import com.google.cloud.bigtable.hbase.async.TestAsyncAdmin;
import com.google.cloud.bigtable.hbase.async.TestAsyncBatch;
import com.google.cloud.bigtable.hbase.async.TestAsyncBufferedMutator;
import com.google.cloud.bigtable.hbase.async.TestAsyncCheckAndMutate;
import com.google.cloud.bigtable.hbase.async.TestAsyncColumnFamily;
import com.google.cloud.bigtable.hbase.async.TestAsyncConnection;
import com.google.cloud.bigtable.hbase.async.TestAsyncCreateTable;
import com.google.cloud.bigtable.hbase.async.TestAsyncScan;
import com.google.cloud.bigtable.hbase.async.TestAsyncSnapshots;
import com.google.cloud.bigtable.hbase.async.TestAsyncTruncateTable;
import com.google.cloud.bigtable.hbase.async.TestBasicAsyncOps;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import org.junit.ClassRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.concurrent.TimeUnit;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestAppend.class,
    TestBasicOps.class,
    TestBatch.class,
    TestBufferedMutator.class,
    TestCheckAndMutate.class,
    TestCheckAndMutateHBase2.class,
    TestCheckAndMutateHBase2Builder.class,
    TestColumnFamilyAdmin.class,
    TestColumnFamilyAdminHBase2.class,
    TestColumnFamilyAdminAsync.class,
    TestCreateTable.class,
    TestCreateTableHBase2.class,
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
    TestListTablesHBase2.class,
    TestPut.class,
    TestTimestamp.class,
    TestTruncateTable.class,
    TestAsyncAdmin.class,
    TestAsyncBatch.class,
    TestAsyncBufferedMutator.class,
    TestAsyncCheckAndMutate.class,
    TestAsyncCreateTable.class,
    TestAsyncConnection.class,
    TestAsyncScan.class,
    TestBasicAsyncOps.class,
    TestAsyncTruncateTable.class,
    TestAsyncSnapshots.class,
    TestAsyncColumnFamily.class,
    TestModifyTable.class
})
public class IntegrationTests {

  private static final int TIME_OUT_MINUTES =
      Integer.getInteger("integration.test.timeout.minutes", 3);

  @ClassRule
  public static Timeout timeoutRule = new Timeout(TIME_OUT_MINUTES, TimeUnit.MINUTES);

  @ClassRule
  public static SharedTestEnvRule sharedTestEnvRule = SharedTestEnvRule.getInstance();

}
