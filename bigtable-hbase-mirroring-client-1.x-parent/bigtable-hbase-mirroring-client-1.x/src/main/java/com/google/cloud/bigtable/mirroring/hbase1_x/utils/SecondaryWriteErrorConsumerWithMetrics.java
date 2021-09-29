/*
 * Copyright 2021 Google LLC
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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import java.util.List;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

public class SecondaryWriteErrorConsumerWithMetrics {
  private final MirroringTracer mirroringTracer;
  private final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer;

  public SecondaryWriteErrorConsumerWithMetrics(
      MirroringTracer mirroringTracer, SecondaryWriteErrorConsumer secondaryWriteErrorConsumer) {
    this.mirroringTracer = mirroringTracer;
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
  }

  public void consume(HBaseOperation operation, Mutation r) {
    this.mirroringTracer.metricsRecorder.recordWriteMismatches(operation, 1);
    this.secondaryWriteErrorConsumer.consume(r);
  }

  public void consume(HBaseOperation operation, RowMutations r) {
    this.mirroringTracer.metricsRecorder.recordWriteMismatches(operation, 1);
    this.secondaryWriteErrorConsumer.consume(r);
  }

  public void consume(HBaseOperation operation, List<? extends Row> operations) {
    this.mirroringTracer.metricsRecorder.recordWriteMismatches(operation, operations.size());
    this.secondaryWriteErrorConsumer.consume(operations);
  }
}
