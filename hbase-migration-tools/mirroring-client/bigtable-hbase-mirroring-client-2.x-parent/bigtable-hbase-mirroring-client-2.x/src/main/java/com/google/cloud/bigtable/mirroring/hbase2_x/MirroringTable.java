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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import com.google.cloud.bigtable.mirroring.core.utils.CallableThrowingIOException;
import com.google.cloud.bigtable.mirroring.core.utils.OperationUtils;
import com.google.cloud.bigtable.mirroring.core.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.core.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.WriteOperationInfo;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.core.verification.MismatchDetector;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;

public class MirroringTable extends com.google.cloud.bigtable.mirroring.core.MirroringTable
    implements Table {
  // We keep a reference to the secondary table to get around the
  // api change for mutate rows (used to return void, not returns Result)
  private Table secondaryTable;

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
      ReferenceCounter referenceCounter,
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
        referenceCounter,
        resultScannerBufferedMismatchedResults);
    this.secondaryTable = secondaryTable;
  }

  @Override
  public TableDescriptor getDescriptor() throws IOException {
    return primaryTable.getDescriptor();
  }

  @Override
  public RegionLocator getRegionLocator() {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public boolean[] exists(List<Get> gets) throws IOException {
    return existsAll(gets);
  }

  @Override
  public Result mutateRow(final RowMutations rowMutations) throws IOException {
    try (Scope scope = this.mirroringTracer.spanFactory.operationScope(HBaseOperation.MUTATE_ROW)) {
      Log.trace("[%s] mutateRow(mutateRow=%s)", this.getName(), rowMutations);

      if (rowMutations.getMutations().isEmpty()) {
        return Result.EMPTY_RESULT;
      }

      Result result =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              new CallableThrowingIOException<Result>() {
                @Override
                public Result call() throws IOException {
                  return primaryTable.mutateRow(rowMutations);
                }
              },
              HBaseOperation.MUTATE_ROW);

      Mutation firstMutation = rowMutations.getMutations().get(0);

      // If it is either Append or Increment, the underlying operation is a rmw and we need
      // the result of that operation to apply on the secondary table
      if (firstMutation instanceof Append || firstMutation instanceof Increment) {
        Put put = OperationUtils.makePutFromResult(result);

        scheduleSequentialWriteOperation(
            new WriteOperationInfo(put), this.secondaryAsyncWrapper.put(put));
      } else {
        // Async wrapper mutateRow implementation returns void. HBase 2.4+ returns result
        // so we skip the syntatic sugar
        scheduleSequentialWriteOperation(
            new WriteOperationInfo(rowMutations),
            this.secondaryAsyncWrapper.createSubmitTaskSupplier(
                new CallableThrowingIOException<Result>() {
                  @Override
                  public Result call() throws IOException {
                    Log.trace("mutateRow(RowMutations)");
                    return secondaryTable.mutateRow(rowMutations);
                  }
                },
                HBaseOperation.MUTATE_ROW));
      }

      return result;
    }
  }

  /**
   * HBase 1.x's {@link Table#append} returns {@code null} when {@link Append#isReturnResults} is
   * {@code false}
   */
  @Override
  public Result append(Append append) throws IOException {
    Result result = super.append(append);
    return result == null ? Result.create(new Cell[0]) : result;
  }
}
