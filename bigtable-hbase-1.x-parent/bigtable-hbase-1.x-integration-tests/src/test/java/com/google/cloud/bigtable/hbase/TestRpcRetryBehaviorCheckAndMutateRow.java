/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

public class TestRpcRetryBehaviorCheckAndMutateRow extends TestRpcRetryBehavior {
  private final AtomicInteger numCheckAndMutateRowInvocations = new AtomicInteger();

  @Override
  protected AtomicInteger getInvocations() {
    // CheckAndMutateRow is not idempotent and does not support retries. Hence, we only expect 1
    // attempt...
    //    assertThat(numCheckAndMutateRowInvocations.get(), equalTo(1));
    // ... Hence, avoid the regular validation method.
    //    validateInvocations(numCheckAndMutateRowInvocations);
    return numCheckAndMutateRowInvocations;
  }

  @Override
  protected void executeLogic(Table table, StopWatch sw) throws Exception {
    try {
      RowMutations rowMutations = new RowMutations("mykey".getBytes());
      rowMutations.add(
          new Put(("mykey").getBytes())
              .addColumn("cf1".getBytes(), "qualifier".getBytes(), "value".getBytes()));
      rowMutations.add(
          new Put(("mykey").getBytes())
              .addColumn("cf1".getBytes(), "qualifier".getBytes(), "value".getBytes()));

      sw.start();
      table.checkAndMutate(
          "mykey".getBytes(),
          "b".getBytes(),
          "c".getBytes(),
          CompareFilter.CompareOp.EQUAL,
          "d".getBytes(),
          rowMutations);

      fail("Should have errored out");
    } catch (Exception e) {
      // Stop ASAP to reduce potential flakiness (due to adding ms to measured query times).
      sw.stop();

      String expectedExceptionMessage = serverRpcAbortsForTest ? "ABORTED" : "DEADLINE_EXCEEDED";
      assertThat(e.getCause().getMessage(), CoreMatchers.containsString(expectedExceptionMessage));
    }
  }

  @Override
  protected ImmutableMap.Builder<String, String> defineProperties() {
    ImmutableMap.Builder<String, String> connPropsBuilder =
        ImmutableMap.<String, String>builder()
            .put(BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY, String.valueOf(timeoutEnabled))
            .put(
                BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY,
                String.valueOf(operationTimeoutMs))
            .put(
                BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY,
                String.valueOf(operationTimeoutMs))
            .put(
                BigtableOptionsFactory.BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY,
                String.valueOf(operationTimeoutMs));

    if (attemptTimeoutEnabled) {
      // Note - the property for CheckAndMutate is not yet defined; only will be if we support
      // retries.
      connPropsBuilder.put(
          BigtableOptionsFactory.BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY,
          String.valueOf(attemptTimeoutMs));

      // Set this to a low number to validate that it is not used when timeouts are enabled.
      connPropsBuilder.put(BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY, "123");
    } else {
      connPropsBuilder.put(
          BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY,
          String.valueOf(maxElapsedBackoffMs));
    }
    return connPropsBuilder;
  }

  @Override
  protected void validateOperationRuntime(StopWatch sw) {
    if (serverRpcAbortsForTest) {
      // CheckAndMutate currently does not retry. So if the server aborts, the RPC errors out
      // sooner per the attempt timeout in our test, not the longer operation timeout.
      assertThat(
          sw.getTime(),
          Matchers.both(Matchers.greaterThan(attemptTimeoutMs))
              .and(Matchers.lessThan(attemptTimeoutMs + 300))); // plus some buffer
    } else {
      super.validateOperationRuntime(sw);
    }
  }

  @Override
  protected void validateInvocations(AtomicInteger counter) {
    // CheckAndMutateRow is not idempotent and does not support retries. Hence, we only expect 1
    // attempt...
    assertThat(numCheckAndMutateRowInvocations.get(), equalTo(1));
  }

  @Override
  protected BigtableGrpc.BigtableImplBase setupRpcCall() {
    return new BigtableGrpc.BigtableImplBase() {
      @Override
      public void checkAndMutateRow(
          CheckAndMutateRowRequest request,
          StreamObserver<CheckAndMutateRowResponse> responseObserver) {
        numCheckAndMutateRowInvocations.incrementAndGet();
      }
    };
  }
}
