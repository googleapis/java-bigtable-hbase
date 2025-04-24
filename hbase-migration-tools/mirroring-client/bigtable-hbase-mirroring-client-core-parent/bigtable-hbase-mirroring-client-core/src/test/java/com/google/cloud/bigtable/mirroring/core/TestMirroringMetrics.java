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
package com.google.cloud.bigtable.mirroring.core;

import static com.google.cloud.bigtable.mirroring.core.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.core.TestHelpers.setupFlowControllerMock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.core.utils.DefaultSecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.core.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.core.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.core.utils.faillog.Appender;
import com.google.cloud.bigtable.mirroring.core.utils.faillog.FailedMutationLogger;
import com.google.cloud.bigtable.mirroring.core.utils.faillog.Serializer;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.NoopTimestamper;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.core.verification.DefaultMismatchDetector;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
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
public class TestMirroringMetrics {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.singleThreadedExecutor();

  @Mock Table primaryTable;
  @Mock Table secondaryTable;
  @Mock FlowController flowController;
  Timestamper timestamper = new NoopTimestamper();

  MirroringTable mirroringTable;

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                this.executorServiceRule.executorService,
                new DefaultMismatchDetector(32),
                flowController,
                    new DefaultSecondaryWriteErrorConsumer(
                        new FailedMutationLogger(mock(Appender.class), mock(Serializer.class))),
                new ReadSampler(100),
                this.timestamper,
                false,
                false,
                mock(ReferenceCounter.class),
                10));
  }

  @Test
  public void testWriteErrorConsumerWithMetricsReportsErrors() {
    SecondaryWriteErrorConsumer secondaryWriteErrorConsumer =
        mock(SecondaryWriteErrorConsumer.class);

    List<Put> puts = Arrays.asList(createPut("r1", "f", "q", "1"), createPut("r2", "f", "q", "v2"));
    secondaryWriteErrorConsumer.consume(HBaseOperation.PUT_LIST, puts, new Throwable());
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.PUT_LIST), eq(puts), any(Throwable.class));
    Put put = createPut("r1", "f", "q", "1");
    secondaryWriteErrorConsumer.consume(HBaseOperation.PUT, put, new Throwable());

    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.PUT), eq(put), any(Throwable.class));

    RowMutations rowMutations = new RowMutations("r1".getBytes());
    secondaryWriteErrorConsumer.consume(
        HBaseOperation.MUTATE_ROW, rowMutations, new Throwable());

    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.MUTATE_ROW), eq(rowMutations), any(Throwable.class));
  }
}
