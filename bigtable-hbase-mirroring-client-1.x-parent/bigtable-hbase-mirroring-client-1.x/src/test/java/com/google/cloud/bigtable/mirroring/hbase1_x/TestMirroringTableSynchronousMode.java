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

import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createPut;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.createResult;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.mockBatch;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerToRejectRequests;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException.DatabaseIdentifier;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestMirroringTableSynchronousMode {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule =
      ExecutorServiceRule.singleThreadedExecutor();

  @Mock Table primaryTable;
  @Mock Table secondaryTable;
  @Mock MismatchDetector mismatchDetector;
  @Mock FlowController flowController;
  @Mock SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;

  MirroringTable mirroringTable;

  @Before
  public void setUp() {
    setupFlowControllerMock(flowController);
    setupTable(false);
  }

  private void setupTable(boolean concurrent) {
    this.mirroringTable =
        spy(
            new MirroringTable(
                primaryTable,
                secondaryTable,
                executorServiceRule.executorService,
                mismatchDetector,
                flowController,
                secondaryWriteErrorConsumer,
                new ReadSampler(100),
                concurrent,
                true,
                new MirroringTracer(),
                mock(ReferenceCounter.class),
                5));
  }

  @Test
  public void testConcurrentFlowControlRejection() throws IOException, InterruptedException {
    setupTable(true);

    IOException flowControllerException = setupFlowControllerToRejectRequests(flowController);

    Put put = createPut("test1", "f1", "q1", "v1");
    try {
      mirroringTable.put(put);
      fail("should throw");
    } catch (IOException e) {
      // FlowController exception is wrapped in IOException by mirroringTable.
      assertThat(e).hasCauseThat().isEqualTo(flowControllerException);
      MirroringOperationException mirroringOperationException =
          MirroringOperationException.extractRootCause(e);
      assertThat(mirroringOperationException).isNotNull();
      assertThat(mirroringOperationException.databaseIdentifier).isEqualTo(DatabaseIdentifier.Both);
      assertThat(mirroringOperationException.operation).isNull();
      assertThat(mirroringOperationException.secondaryException).isNull();
    }

    verify(primaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  @Test
  public void testConcurrentWithoutErrors() throws IOException, InterruptedException {
    setupTable(true);

    Put put = createPut("test1", "f1", "q1", "v1");

    mockBatch(primaryTable, secondaryTable);

    mirroringTable.put(put);

    verify(primaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  @Test
  public void testConcurrentWithErrors() throws IOException, InterruptedException {
    setupTable(true);

    Put put1 = createPut("test1", "f1", "q1", "v1");
    Put put2 = createPut("test2", "f1", "q1", "v2");
    Put put3 = createPut("test3", "f1", "q1", "v3");
    Put put4 = createPut("test4", "f1", "q1", "v4");

    List<? extends Row> operations = Arrays.asList(put1, put2, put3, put4);

    IOException put1error = new IOException("put1");
    IOException put2error = new IOException("put2");
    IOException put3error1 = new IOException("put3_1");
    IOException put3error2 = new IOException("put3_2");

    //    | p1 | p2 | p3 | p4
    // 1  | x  | v  | x  | v
    // 2  | v  | x  | x  | v

    mockBatch(primaryTable, put1, put1error, put3, put3error1);
    mockBatch(secondaryTable, put2, put2error, put3, put3error2);

    Object[] results = new Object[4];

    RetriesExhaustedWithDetailsException exception = null;
    try {
      mirroringTable.batch(operations, results);
      fail("should throw");
    } catch (RetriesExhaustedWithDetailsException e) {
      exception = e;
    }

    assertThat(results[0]).isInstanceOf(Throwable.class);
    assertThat(results[1]).isInstanceOf(Throwable.class);
    assertThat(results[2]).isInstanceOf(Throwable.class);
    assertThat(results[3]).isNotInstanceOf(Throwable.class);

    Throwable t1 = (Throwable) results[0];
    Throwable t2 = (Throwable) results[1];
    Throwable t3 = (Throwable) results[2];

    assertThat(t1).isEqualTo(put1error);
    assertThat(t2).isEqualTo(put2error);
    assertThat(t3).isEqualTo(put3error1);

    assertThat(MirroringOperationException.extractRootCause(t1).databaseIdentifier)
        .isEqualTo(DatabaseIdentifier.Primary);
    assertThat(MirroringOperationException.extractRootCause(t2).databaseIdentifier)
        .isEqualTo(DatabaseIdentifier.Secondary);
    assertThat(MirroringOperationException.extractRootCause(t3).databaseIdentifier)
        .isEqualTo(DatabaseIdentifier.Both);
    assertThat(MirroringOperationException.extractRootCause(t3).secondaryException.exception)
        .isEqualTo(put3error2);

    assertThat(exception.getNumExceptions()).isEqualTo(3);

    assertThat(exception.getRow(0)).isEqualTo(put1);
    assertThat(exception.getCause(0)).isEqualTo(put1error);
    assertThat(exception.getRow(1)).isEqualTo(put2);
    assertThat(exception.getCause(1)).isEqualTo(put2error);
    assertThat(exception.getRow(2)).isEqualTo(put3);
    assertThat(exception.getCause(2)).isEqualTo(put3error1);
    assertThat(
            MirroringOperationException.extractRootCause(exception.getCause(2))
                .secondaryException
                .exception)
        .isEqualTo(put3error2);

    verify(primaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  @Test
  public void testSequentialFlowControlRejection() throws IOException, InterruptedException {
    setupTable(false);

    IOException flowControllerException = setupFlowControllerToRejectRequests(flowController);

    mockBatch(primaryTable);

    Put put = createPut("test1", "f1", "q1", "v1");
    Object[] results = new Object[1];
    try {
      mirroringTable.batch(Arrays.asList(put), results);
      fail("should throw");
    } catch (RetriesExhaustedWithDetailsException e) {
      // FlowController exception is wrapped in IOException by mirroringTable and in
      // ExecutionException by a future.
      assertThat(e.getNumExceptions()).isEqualTo(1);
      assertThat(e.getCause(0)).isEqualTo(flowControllerException);
      MirroringOperationException mirroringException =
          MirroringOperationException.extractRootCause(e.getCause(0));
      assertThat(mirroringException.operation).isEqualTo(put);
      assertThat(mirroringException.databaseIdentifier).isEqualTo(DatabaseIdentifier.Secondary);
      assertThat(mirroringException.secondaryException).isNull();
    }

    verify(primaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryTable, never()).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  @Test
  public void testSequentialWithoutErrors() throws IOException, InterruptedException {
    setupTable(false);

    Put put = createPut("test1", "f1", "q1", "v1");

    mockBatch(primaryTable, secondaryTable);

    mirroringTable.put(put);

    verify(primaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }

  @Test
  public void testSequentialWithErrors() throws IOException, InterruptedException {
    setupTable(false);

    Put put1 = createPut("test1", "f1", "q1", "v1");
    Increment incr2 = new Increment("test2".getBytes());
    incr2.addColumn("f1".getBytes(), "q1".getBytes(), 1);
    Put put3 = createPut("test3", "f1", "q1", "v3");
    Put put4 = createPut("test4", "f1", "q1", "v4");

    List<? extends Row> operations = Arrays.asList(put1, incr2, put3, put4);

    IOException put1error = new IOException("put1");
    IOException put2error = new IOException("incr2");
    IOException put3error1 = new IOException("put3_1");
    IOException put3error2 = new IOException("put3_2");

    //    | p1 | p2 | p3 | p4
    // 1  | x  | v  | x  | v
    // 2  | v  | x  | x  | v

    mockBatch(
        primaryTable,
        put1,
        put1error,
        incr2,
        createResult("test2", "f1", "q1", 123, "v11"),
        put3,
        put3error1);
    mockBatch(
        secondaryTable,
        Put.class,
        put2error,
        put3,
        put3error2,
        put1,
        new Result(),
        put4,
        new Result());

    Object[] results = new Object[4];

    RetriesExhaustedWithDetailsException exception = null;
    try {
      mirroringTable.batch(operations, results);
      fail("should throw");
    } catch (RetriesExhaustedWithDetailsException e) {
      exception = e;
    }

    assertThat(results[0]).isInstanceOf(Throwable.class);
    assertThat(results[1]).isInstanceOf(Throwable.class);
    assertThat(results[2]).isInstanceOf(Throwable.class);
    assertThat(results[3]).isNotInstanceOf(Throwable.class);

    Throwable t1 = (Throwable) results[0];
    Throwable t2 = (Throwable) results[1];
    Throwable t3 = (Throwable) results[2];

    assertThat(t1).isEqualTo(put1error);
    assertThat(t2).isEqualTo(put2error);
    assertThat(t3).isEqualTo(put3error1);

    assertThat(MirroringOperationException.extractRootCause(t1).databaseIdentifier)
        .isEqualTo(DatabaseIdentifier.Primary);
    assertThat(MirroringOperationException.extractRootCause(t2).databaseIdentifier)
        .isEqualTo(DatabaseIdentifier.Secondary);
    assertThat(MirroringOperationException.extractRootCause(t2).operation).isInstanceOf(Put.class);
    assertThat(MirroringOperationException.extractRootCause(t3).databaseIdentifier)
        .isEqualTo(DatabaseIdentifier.Primary); // Sequential - not run on secondary
    assertThat(MirroringOperationException.extractRootCause(t3).secondaryException).isNull();

    assertThat(exception.getNumExceptions()).isEqualTo(3);

    assertThat(exception.getRow(0)).isEqualTo(put1);
    assertThat(exception.getCause(0)).isEqualTo(put1error);
    assertThat(exception.getRow(1)).isEqualTo(incr2);
    assertThat(exception.getCause(1)).isEqualTo(put2error);
    assertThat(exception.getRow(2)).isEqualTo(put3);
    assertThat(exception.getCause(2)).isEqualTo(put3error1);
    assertThat(
            MirroringOperationException.extractRootCause(exception.getCause(2)).secondaryException)
        .isNull();

    verify(primaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
    verify(secondaryTable, times(1)).batch(ArgumentMatchers.<Row>anyList(), any(Object[].class));
  }
}
