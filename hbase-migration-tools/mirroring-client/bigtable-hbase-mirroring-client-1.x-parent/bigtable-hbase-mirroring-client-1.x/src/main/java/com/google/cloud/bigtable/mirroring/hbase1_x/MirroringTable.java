/*
 * Copyright 2025 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.core.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.core.verification.MismatchDetector;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;

@InternalApi("For internal usage only")
public class MirroringTable extends com.google.cloud.bigtable.mirroring.core.MirroringTable
    implements Table {
  /**
   * @param executorService ExecutorService is used to perform operations on secondaryTable and
   *     verification tasks.
   * @param mismatchDetector Detects mismatches in results from operations preformed on both
   *     databases.
   * @param secondaryWriteErrorConsumer Consumer secondary write errors.
   */
  public MirroringTable(
      Table primaryTable,
      Table secondaryTable,
      ExecutorService executorService,
      MismatchDetector mismatchDetector,
      FlowController flowController,
      SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      ReadSampler readSampler,
      Timestamper timestamper,
      boolean performWritesConcurrently,
      boolean waitForSecondaryWrites,
      MirroringTracer mirroringTracer,
      ReferenceCounter parentReferenceCounter,
      int resultScannerBufferedMismatchedResults) {
    super(
        primaryTable,
        secondaryTable,
        executorService,
        mismatchDetector,
        flowController,
        secondaryWriteErrorConsumer,
        readSampler,
        timestamper,
        performWritesConcurrently,
        waitForSecondaryWrites,
        mirroringTracer,
        parentReferenceCounter,
        resultScannerBufferedMismatchedResults);
  }

  @Override
  public void mutateRow(final RowMutations rowMutations) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.MUTATE_ROW)) {
      Log.trace("[%s] mutateRow(rowMutations=%s)", this.getName(), rowMutations);
      this.batcher.batchSingleWriteOperation(rowMutations);
    }
  }
}
