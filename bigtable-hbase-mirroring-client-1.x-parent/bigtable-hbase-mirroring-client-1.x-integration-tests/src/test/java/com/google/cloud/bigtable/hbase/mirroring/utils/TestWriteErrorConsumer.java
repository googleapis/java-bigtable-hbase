/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import java.util.List;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

public class TestWriteErrorConsumer implements SecondaryWriteErrorConsumer {
  static int errorCount;

  public static int getErrorCount() {
    return errorCount;
  }

  public static void clearErrors() {
    errorCount = 0;
  }

  @Override
  public void consume(Mutation r) {
    errorCount++;
  }

  @Override
  public void consume(RowMutations r) {
    errorCount++;
  }

  @Override
  public void consume(List<? extends Row> operations) {
    for (Row operation : operations) {
      assert operation instanceof Mutation || operation instanceof RowMutations;
    }
    errorCount += operations.size();
  }
}
