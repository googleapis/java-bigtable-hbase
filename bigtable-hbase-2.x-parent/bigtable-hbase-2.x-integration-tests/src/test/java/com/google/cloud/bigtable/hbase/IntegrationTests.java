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

import com.google.cloud.bigtable.hbase.async.ITAsyncAdmin;
import com.google.cloud.bigtable.hbase.async.ITAsyncBatch;
import com.google.cloud.bigtable.hbase.async.ITAsyncBufferedMutator;
import com.google.cloud.bigtable.hbase.async.ITAsyncCheckAndMutate;
import com.google.cloud.bigtable.hbase.async.ITAsyncCreateTable;
import com.google.cloud.bigtable.hbase.async.ITAsyncColumnFamily;
import com.google.cloud.bigtable.hbase.async.ITAsyncConnection;
import com.google.cloud.bigtable.hbase.async.ITAsyncScan;
import com.google.cloud.bigtable.hbase.async.ITAsyncSnapshots;
import com.google.cloud.bigtable.hbase.async.ITAsyncTruncateTable;
import com.google.cloud.bigtable.hbase.async.ITBasicAsyncOps;
import com.google.cloud.bigtable.hbase.async.ITModifyTableAsync;
import com.google.cloud.bigtable.hbase.test_env.SharedTestEnvRule;
import java.util.concurrent.TimeUnit;
import org.junit.ClassRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
  ITAppend.class,
  ITBasicOps.class,
  ITBatch.class,
  ITBufferedMutator.class,
  ITCheckAndMutate.class,
  ITCheckAndMutateHBase2.class,
  ITCheckAndMutateHBase2Builder.class,
  ITColumnFamilyAdmin.class,
  ITColumnFamilyAdminHBase2.class,
  ITColumnFamilyAdminAsync.class,
  ITCreateTable.class,
  ITCreateTableHBase2.class,
  ITDisableTable.class,
  ITDelete.class,
  ITDurability.class,
  ITFilters.class,
  ITSingleColumnValueFilter.class,
  ITGet.class,
  ITGetTable.class,
  ITScan.class,
  ITSnapshots.class,
  ITIncrement.class,
  ITListTables.class,
  ITListTablesHBase2.class,
  ITPut.class,
  ITTimestamp.class,
  ITTruncateTable.class,
  ITAsyncAdmin.class,
  ITAsyncBatch.class,
  ITAsyncBufferedMutator.class,
  ITAsyncCheckAndMutate.class,
  ITAsyncCreateTable.class,
  ITAsyncConnection.class,
  ITAsyncScan.class,
  ITBasicAsyncOps.class,
  ITAsyncTruncateTable.class,
  ITAsyncSnapshots.class,
  ITAsyncColumnFamily.class,
  ITModifyTable.class,
  ITModifyTableAsync.class
})
public class IntegrationTests {

  private static final int TIME_OUT_MINUTES =
      Integer.getInteger("integration.test.timeout.minutes", 3);

  @ClassRule public static Timeout timeoutRule = new Timeout(TIME_OUT_MINUTES, TimeUnit.MINUTES);

  @ClassRule public static SharedTestEnvRule sharedTestEnvRule = SharedTestEnvRule.getInstance();
}
