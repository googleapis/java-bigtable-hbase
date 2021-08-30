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
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestRpcRetryBehaviorPutMultiple extends TestRpcRetryBehavior {
  private final AtomicInteger numMutateRowsInvocations = new AtomicInteger();

  @Override
  protected AtomicInteger getInvocations() {
    return numMutateRowsInvocations;
  }

  @Override
  protected void executeLogic(Table table, StopWatch sw) throws Exception {
    try {
      sw.start();
      table.put(
          Arrays.asList(
              new Put(("mykey1").getBytes())
                  .addColumn("cf1".getBytes(), "qualifier".getBytes(), "value".getBytes()),
              new Put(("mykey2").getBytes())
                  .addColumn("cf1".getBytes(), "qualifier".getBytes(), "value".getBytes()),
              new Put(("mykey3").getBytes())
                  .addColumn("cf1".getBytes(), "qualifier".getBytes(), "value".getBytes())));

      fail("Should have errored out");
    } catch (RetriesExhaustedWithDetailsException e) {
      // Stop ASAP to reduce potential flakiness (due to adding ms to measured query times).
      sw.stop();

      // Expect this specific error class above.
      // As a sanity check for our test, verify that the exception count matches the number of
      // puts (this exact
      // verification is unnecessary for the retry functionality).
      assertThat(e.getNumExceptions(), equalTo(3));
    }
  }

  @Override
  protected ImmutableMap.Builder<String, String> defineProperties() {
    ImmutableMap.Builder<String, String> connPropsBuilder =
        ImmutableMap.<String, String>builder()
            .put(BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY, String.valueOf(timeoutEnabled))
            .put(
                BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY,
                String.valueOf(operationTimeoutMs));

    if (attemptTimeoutEnabled) {
      // The regular RPC attempt timeout covers a singular MutateRow RPC.
      // Use the mutate timeout instead.
      connPropsBuilder.put(
          BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_ATTEMPT_TIMEOUT_MS_KEY,
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
  protected BigtableGrpc.BigtableImplBase setupRpcCall() {
    return new BigtableGrpc.BigtableImplBase() {
      @Override
      public void mutateRows(
          MutateRowsRequest request, StreamObserver<MutateRowsResponse> responseObserver) {
        numMutateRowsInvocations.incrementAndGet();
      }
    };
  }
}
