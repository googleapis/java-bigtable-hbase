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

import com.google.cloud.bigtable.mirroring.core.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.core.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.core.verification.MismatchDetector;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;

public class MirroringTable extends com.google.cloud.bigtable.mirroring.core.MirroringTable
    implements Table {
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
