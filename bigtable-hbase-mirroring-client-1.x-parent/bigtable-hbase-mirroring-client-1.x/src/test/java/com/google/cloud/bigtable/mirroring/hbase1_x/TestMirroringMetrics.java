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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createGet;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createResult;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.mockBatch;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.FLOW_CONTROL_LATENCY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.MIRRORING_LATENCY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.PRIMARY_ERRORS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.PRIMARY_LATENCY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.SECONDARY_ERRORS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.SECONDARY_LATENCY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.SECONDARY_WRITE_ERROR_HANDLER_LATENCY;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.DefaultSecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.Appender;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.FailedMutationLogger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.Serializer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringMetricsRecorder;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanFactory;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper.NoopTimestamper;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.DefaultMismatchDetector;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

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

  @Mock MirroringMetricsRecorder mirroringMetricsRecorder;

  MirroringTable mirroringTable;

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    MirroringSpanFactory mirroringSpanFactory =
        new MirroringSpanFactory(Tracing.getTracer(), mirroringMetricsRecorder);
    MirroringTracer tracer = new MirroringTracer(mirroringSpanFactory, mirroringMetricsRecorder);
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                this.executorServiceRule.executorService,
                new DefaultMismatchDetector(tracer, 32),
                flowController,
                new SecondaryWriteErrorConsumerWithMetrics(
                    tracer,
                    new DefaultSecondaryWriteErrorConsumer(
                        new FailedMutationLogger(
                            tracer, mock(Appender.class), mock(Serializer.class)))),
                new ReadSampler(100),
                this.timestamper,
                false,
                false,
                tracer,
                mock(ReferenceCounter.class),
                10));
  }

  @Test
  public void testOperationLatenciesAreRecorded() throws IOException {
    Get get = createGet("test");
    Result result1 = createResult("test", "value");

    when(primaryTable.get(get)).thenReturn(result1);
    when(secondaryTable.get(get)).thenReturn(result1);

    mirroringTable.get(get);
    executorServiceRule.waitForExecutor();

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.GET), eq(PRIMARY_LATENCY), anyLong(), eq(PRIMARY_ERRORS), eq(false));

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.GET),
            eq(SECONDARY_LATENCY),
            anyLong(),
            eq(SECONDARY_ERRORS),
            eq(false));

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(eq(HBaseOperation.GET), eq(MIRRORING_LATENCY), anyLong());

    verify(mirroringMetricsRecorder, times(1))
        .recordReadMismatches(any(HBaseOperation.class), eq(0));
    verify(mirroringMetricsRecorder, never())
        .recordSecondaryWriteErrors(any(HBaseOperation.class), anyInt());
  }

  @Test
  public void testReadMismatchIsRecorded() throws IOException {
    Get get = createGet("test");
    Result result1 = createResult("test", "value1");
    Result result2 = createResult("test", "value2");

    when(primaryTable.get(get)).thenReturn(result1);
    when(secondaryTable.get(get)).thenReturn(result2);

    mirroringTable.get(get);
    executorServiceRule.waitForExecutor();

    verify(mirroringMetricsRecorder, times(1)).recordReadMismatches(HBaseOperation.GET, 1);
    verify(mirroringMetricsRecorder, never())
        .recordSecondaryWriteErrors(any(HBaseOperation.class), anyInt());
  }

  @Test
  public void testMatchingReadReportsZeroMismatches() throws IOException {
    Get get = createGet("test");
    Result result1 = createResult("test", "value1");

    when(primaryTable.get(get)).thenReturn(result1);
    when(secondaryTable.get(get)).thenReturn(result1);

    mirroringTable.get(get);
    executorServiceRule.waitForExecutor();

    verify(mirroringMetricsRecorder, times(1)).recordReadMismatches(HBaseOperation.GET, 0);
    verify(mirroringMetricsRecorder, never())
        .recordSecondaryWriteErrors(any(HBaseOperation.class), anyInt());
  }

  @Test
  public void testSuccessfulConditionalWriteReportsZeroFailures() throws IOException {
    byte[] row = new byte[] {1};
    byte[] family = new byte[] {2};
    byte[] qualifier = new byte[] {3};
    byte[] value = new byte[] {4};

    Put put = new Put(row);
    put.addColumn(family, qualifier, value);

    RowMutations rm = new RowMutations(row);
    rm.add(put);

    when(primaryTable.checkAndMutate(row, family, qualifier, CompareOp.EQUAL, value, rm))
        .thenReturn(true);

    mirroringTable.checkAndPut(row, family, qualifier, CompareOp.EQUAL, value, put);
    executorServiceRule.waitForExecutor();

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.CHECK_AND_MUTATE),
            eq(PRIMARY_LATENCY),
            anyLong(),
            eq(PRIMARY_ERRORS),
            eq(false));
    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.MUTATE_ROW),
            eq(SECONDARY_LATENCY),
            anyLong(),
            eq(SECONDARY_ERRORS),
            eq(false));
    verify(mirroringMetricsRecorder, times(1))
        .recordSecondaryWriteErrors(HBaseOperation.MUTATE_ROW, 0);
  }

  @Test
  public void testSuccessfulWriteReportsZeroFailures() throws IOException, InterruptedException {
    Put put = createPut("test", "f1", "q1", "v1");

    mockBatch(primaryTable, put, new Result());
    mockBatch(secondaryTable, put, new Result());

    mirroringTable.put(put);
    executorServiceRule.waitForExecutor();

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.BATCH),
            eq(PRIMARY_LATENCY),
            anyLong(),
            eq(PRIMARY_ERRORS),
            eq(false));
    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.BATCH),
            eq(SECONDARY_LATENCY),
            anyLong(),
            eq(SECONDARY_ERRORS),
            eq(false));
    verify(mirroringMetricsRecorder, times(1)).recordSecondaryWriteErrors(HBaseOperation.BATCH, 0);
  }

  @Test
  public void testPrimaryErrorMetricIsRecorded() throws IOException {
    Get request = createGet("test");
    Result expectedResult = createResult("test", "value");

    IOException expectedException = new IOException("expected");
    when(primaryTable.get(request)).thenThrow(expectedException);

    try {
      mirroringTable.get(request);
      fail("should throw");
    } catch (IOException ignore) {

    }
    executorServiceRule.waitForExecutor();

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.GET), eq(PRIMARY_LATENCY), anyLong(), eq(PRIMARY_ERRORS), eq(true));

    verify(mirroringMetricsRecorder, never())
        .recordOperation(
            any(HBaseOperation.class),
            eq(SECONDARY_LATENCY),
            anyLong(),
            eq(SECONDARY_ERRORS),
            anyBoolean());

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(eq(HBaseOperation.GET), eq(MIRRORING_LATENCY), anyLong());

    verify(mirroringMetricsRecorder, never())
        .recordReadMismatches(any(HBaseOperation.class), anyInt());
    verify(mirroringMetricsRecorder, never())
        .recordSecondaryWriteErrors(any(HBaseOperation.class), anyInt());
  }

  @Test
  public void testSecondaryErrorMetricIsRecorded() throws IOException {
    Get request = createGet("test");
    Result expectedResult = createResult("test", "value");

    when(primaryTable.get(request)).thenReturn(expectedResult);
    IOException expectedException = new IOException("expected");
    when(secondaryTable.get(request)).thenThrow(expectedException);

    mirroringTable.get(request);
    executorServiceRule.waitForExecutor();

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.GET), eq(PRIMARY_LATENCY), anyLong(), eq(PRIMARY_ERRORS), eq(false));

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.GET),
            eq(SECONDARY_LATENCY),
            anyLong(),
            eq(SECONDARY_ERRORS),
            eq(true));

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(eq(HBaseOperation.GET), eq(MIRRORING_LATENCY), anyLong());

    verify(mirroringMetricsRecorder, never())
        .recordReadMismatches(any(HBaseOperation.class), anyInt());
    verify(mirroringMetricsRecorder, never())
        .recordSecondaryWriteErrors(any(HBaseOperation.class), anyInt());
  }

  @Test
  public void testSingleWriteErrorMetricIsRecorded() throws IOException, InterruptedException {
    Put put = createPut("test", "f1", "q1", "v1");

    mockBatch(primaryTable, put, new Result());
    mockBatch(secondaryTable, put, new IOException("test exception"));

    mirroringTable.put(put);
    executorServiceRule.waitForExecutor();

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.BATCH),
            eq(PRIMARY_LATENCY),
            anyLong(),
            eq(PRIMARY_ERRORS),
            eq(false));

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.BATCH),
            eq(SECONDARY_LATENCY),
            anyLong(),
            eq(SECONDARY_ERRORS),
            eq(true));

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(eq(HBaseOperation.PUT), eq(MIRRORING_LATENCY), anyLong());

    verify(mirroringMetricsRecorder, times(1)).recordLatency(eq(FLOW_CONTROL_LATENCY), anyLong());
    verify(mirroringMetricsRecorder, times(1))
        .recordLatency(eq(SECONDARY_WRITE_ERROR_HANDLER_LATENCY), anyLong());

    verify(mirroringMetricsRecorder, never())
        .recordReadMismatches(any(HBaseOperation.class), anyInt());
    verify(mirroringMetricsRecorder, times(1)).recordSecondaryWriteErrors(HBaseOperation.BATCH, 1);
  }

  @Test
  public void testMultipleWriteErrorMetricIsRecorded() throws IOException, InterruptedException {
    Put put1 = createPut("test", "f1", "q1", "v1");
    Put put2 = createPut("test", "f1", "q1", "v1");
    Put put3 = createPut("test", "f1", "q1", "v1");
    List<Put> put = new ArrayList<>();
    put.add(put1);
    put.add(put2);
    put.add(put3);

    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Object[] result = (Object[]) args[1];

                result[0] = Result.create(new Cell[0]);
                result[1] = Result.create(new Cell[0]);
                result[2] = Result.create(new Cell[0]);
                return null;
              }
            })
        .when(primaryTable)
        .batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                Object[] args = invocationOnMock.getArguments();
                Object[] result = (Object[]) args[1];

                result[0] = Result.create(new Cell[0]);
                result[1] = null;
                result[2] = null;
                throw new RetriesExhaustedWithDetailsException("test");
              }
            })
        .when(secondaryTable)
        .batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));

    mirroringTable.put(put);
    executorServiceRule.waitForExecutor();

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.BATCH),
            eq(PRIMARY_LATENCY),
            anyLong(),
            eq(PRIMARY_ERRORS),
            eq(false));

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(
            eq(HBaseOperation.BATCH),
            eq(SECONDARY_LATENCY),
            anyLong(),
            eq(SECONDARY_ERRORS),
            eq(true));

    verify(mirroringMetricsRecorder, times(1))
        .recordOperation(eq(HBaseOperation.PUT_LIST), eq(MIRRORING_LATENCY), anyLong());

    verify(mirroringMetricsRecorder, never())
        .recordReadMismatches(any(HBaseOperation.class), anyInt());
    verify(mirroringMetricsRecorder, times(2)).recordSecondaryWriteErrors(HBaseOperation.BATCH, 1);
  }

  @Test
  public void testWriteErrorConsumerWithMetricsReportsErrors() {
    MirroringMetricsRecorder mirroringMetricsRecorder = mock(MirroringMetricsRecorder.class);
    MirroringTracer mirroringTracer =
        new MirroringTracer(
            new MirroringSpanFactory(Tracing.getTracer(), mirroringMetricsRecorder),
            mirroringMetricsRecorder);

    SecondaryWriteErrorConsumer secondaryWriteErrorConsumer =
        mock(SecondaryWriteErrorConsumer.class);
    SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumerWithMetrics =
        new SecondaryWriteErrorConsumerWithMetrics(mirroringTracer, secondaryWriteErrorConsumer);

    List<Put> puts = Arrays.asList(createPut("r1", "f", "q", "1"), createPut("r2", "f", "q", "v2"));
    secondaryWriteErrorConsumerWithMetrics.consume(HBaseOperation.PUT_LIST, puts, new Throwable());
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.PUT_LIST), eq(puts), any(Throwable.class));
    verify(mirroringMetricsRecorder, times(1))
        .recordSecondaryWriteErrors(HBaseOperation.PUT_LIST, 2);

    Put put = createPut("r1", "f", "q", "1");
    secondaryWriteErrorConsumerWithMetrics.consume(HBaseOperation.PUT, put, new Throwable());

    verify(mirroringMetricsRecorder, times(1)).recordSecondaryWriteErrors(HBaseOperation.PUT, 1);
    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.PUT), eq(put), any(Throwable.class));

    RowMutations rowMutations = new RowMutations("r1".getBytes());
    secondaryWriteErrorConsumerWithMetrics.consume(
        HBaseOperation.MUTATE_ROW, rowMutations, new Throwable());

    verify(secondaryWriteErrorConsumer, times(1))
        .consume(eq(HBaseOperation.MUTATE_ROW), eq(rowMutations), any(Throwable.class));
    verify(mirroringMetricsRecorder, times(1))
        .recordSecondaryWriteErrors(HBaseOperation.MUTATE_ROW, 1);
  }
}
