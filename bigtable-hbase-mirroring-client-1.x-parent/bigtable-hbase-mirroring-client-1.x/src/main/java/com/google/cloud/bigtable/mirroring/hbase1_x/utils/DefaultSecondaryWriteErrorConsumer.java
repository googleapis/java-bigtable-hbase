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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import java.util.List;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

public class DefaultSecondaryWriteErrorConsumer implements SecondaryWriteErrorConsumer {
  private static final com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger Log =
      new com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger(
          DefaultSecondaryWriteErrorConsumer.class);
  private final Logger writeErrorLogger;

  public DefaultSecondaryWriteErrorConsumer(Logger writeErrorLogger) {
    this.writeErrorLogger = writeErrorLogger;
  }

  @Override
  public void consume(HBaseOperation operation, Mutation r, Throwable cause) {
    try {
      writeErrorLogger.mutationFailed(r, cause);
    } catch (InterruptedException e) {
      Log.error(
          "Writing mutation that failed on secondary database to faillog interrupted: mutation=%s, failure_cause=%s, exception=%s",
          r, cause, e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void consume(HBaseOperation operation, RowMutations r, Throwable cause) {
    for (Mutation m : r.getMutations()) {
      try {
        writeErrorLogger.mutationFailed(m, cause);
      } catch (InterruptedException e) {
        Log.error(
            "Writing mutation that failed on secondary database to faillog interrupted: mutation=%s, failure_cause=%s, exception=%s",
            r, cause, e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void consume(HBaseOperation operation, List<? extends Row> operations, Throwable cause) {
    for (Row row : operations) {
      if (row instanceof Mutation) {
        consume(operation, (Mutation) row, cause);
      } else if (row instanceof RowMutations) {
        consume(operation, (RowMutations) row, cause);
      } else {
        assert false;
        throw new IllegalArgumentException("Not a write operation");
      }
    }
  }
}
