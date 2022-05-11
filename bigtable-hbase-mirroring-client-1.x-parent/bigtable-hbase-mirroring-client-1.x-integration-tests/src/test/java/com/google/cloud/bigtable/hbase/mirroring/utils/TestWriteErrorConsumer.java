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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.DefaultSecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

public class TestWriteErrorConsumer implements SecondaryWriteErrorConsumer {
  static AtomicInteger errorCount = new AtomicInteger(0);
  private final DefaultSecondaryWriteErrorConsumer secondaryWriteErrorConsumer;

  public TestWriteErrorConsumer(Logger writeErrorLogger) {
    this.secondaryWriteErrorConsumer = new DefaultSecondaryWriteErrorConsumer(writeErrorLogger);
  }

  public static int getErrorCount() {
    return errorCount.get();
  }

  public static void clearErrors() {
    errorCount.set(0);
  }

  @Override
  public void consume(HBaseOperation operation, Row row, Throwable cause) {
    assert row instanceof Mutation || row instanceof RowMutations;
    errorCount.addAndGet(1);
    this.secondaryWriteErrorConsumer.consume(operation, row, cause);
  }

  @Override
  public void consume(HBaseOperation operation, List<? extends Row> operations, Throwable cause) {
    for (Row row : operations) {
      assert row instanceof Mutation || row instanceof RowMutations;
    }
    errorCount.addAndGet(operations.size());
    this.secondaryWriteErrorConsumer.consume(operation, operations, cause);
  }
}
