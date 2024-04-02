/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.bigtable.hbase.mirroring.utils.ConfigurationHelper;
import com.google.cloud.bigtable.hbase.mirroring.utils.ConnectionRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.DatabaseHelpers.DatabaseSelector;
import com.google.cloud.bigtable.hbase.mirroring.utils.Helpers;
import com.google.cloud.bigtable.hbase.mirroring.utils.MismatchDetectorCounterRule;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestMismatchDetectorCounter;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestWriteErrorConsumer;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion;
import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegionRule;
import com.google.cloud.bigtable.mirroring.core.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.core.MirroringConnection;
import com.google.cloud.bigtable.mirroring.core.MirroringOptions;
import com.google.cloud.bigtable.mirroring.core.utils.Comparators;
import com.google.common.base.Predicate;
import com.google.common.primitives.Longs;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.shaded.org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/** Tests solely for Hbase 1x */
public class TestMirroringTable1x {
  @ClassRule public static ConnectionRule connectionRule = new ConnectionRule();
  @Rule public ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();
  private DatabaseHelpers databaseHelpers =
      new DatabaseHelpers(connectionRule, executorServiceRule);

  @Rule
  public MismatchDetectorCounterRule mismatchDetectorCounterRule =
      new MismatchDetectorCounterRule();

  @Rule public FailingHBaseHRegionRule failingHBaseHRegionRule = new FailingHBaseHRegionRule();

  static final byte[] columnFamily1 = "cf1".getBytes();
  static final byte[] qualifier1 = "cq1".getBytes();

  // HBase 2.4 changed the return type from void -> Result
  @Test
  public void testMutateRow() throws IOException {
    int databaseEntriesCount = 1000;

    final TableName tableName = connectionRule.createTable(columnFamily1);
    try (MirroringConnection connection = databaseHelpers.createConnection()) {
      try (Table t1 = connection.getTable(tableName)) {
        for (int i = 0; i < databaseEntriesCount; i++) {
          byte[] rowKey = rowKeyFromId(i);
          t1.mutateRow(
              Helpers.createRowMutations(
                  rowKey,
                  Helpers.createPut(i, columnFamily1, qualifier1)
              )
          );
        }
      }
    }

    databaseHelpers.verifyTableConsistency(tableName);
  }

  public static byte[] rowKeyFromId(int id) {
    return Longs.toByteArray(id);
  }
}
