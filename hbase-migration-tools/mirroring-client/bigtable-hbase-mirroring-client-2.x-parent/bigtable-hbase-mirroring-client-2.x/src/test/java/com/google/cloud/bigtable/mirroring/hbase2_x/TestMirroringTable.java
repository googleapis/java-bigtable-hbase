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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import static com.google.cloud.bigtable.mirroring.core.TestHelpers.setupFlowControllerMock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.core.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.core.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.core.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringMetricsRecorder;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanFactory;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.NoopTimestamper;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.core.verification.DefaultMismatchDetector;
import com.google.cloud.bigtable.mirroring.core.verification.MismatchDetector;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestMirroringTable {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.singleThreadedExecutor();

  @Mock Table primaryTable;
  @Mock Table secondaryTable;
  @Mock FlowController flowController;
  @Mock MirroringMetricsRecorder mirroringMetricsRecorder;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  @Mock ReferenceCounter referenceCounter;

  Timestamper timestamper = new NoopTimestamper();
  MismatchDetector mismatchDetector;
  MirroringTable mirroringTable;
  MirroringTracer mirroringTracer;

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    this.mirroringTracer =
        new MirroringTracer(
            new MirroringSpanFactory(Tracing.getTracer(), mirroringMetricsRecorder),
            mirroringMetricsRecorder);
    this.mismatchDetector = spy(new DefaultMismatchDetector(this.mirroringTracer, 100));
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                this.executorServiceRule.executorService,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                new ReadSampler(100),
                this.timestamper,
                false,
                false,
                this.mirroringTracer,
                this.referenceCounter,
                1000));
  }

  @Test
  public void testMutateRow() throws IOException, InterruptedException {
    RowMutations mutations = new RowMutations("r1".getBytes());
    mutations.add(new Put("r1".getBytes()));
    when(primaryTable.mutateRow(any(RowMutations.class))).thenReturn(Result.EMPTY_RESULT);
    mirroringTable.mutateRow(mutations);
    executorServiceRule.waitForExecutor();
    verify(primaryTable, times(1)).mutateRow(mutations);
    verify(secondaryTable, times(1)).mutateRow(mutations);
  }
}
