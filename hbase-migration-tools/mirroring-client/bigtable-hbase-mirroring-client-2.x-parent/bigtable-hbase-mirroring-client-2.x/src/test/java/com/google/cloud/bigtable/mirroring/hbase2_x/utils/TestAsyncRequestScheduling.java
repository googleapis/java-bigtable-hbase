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
package com.google.cloud.bigtable.mirroring.hbase2_x.utils;

import static com.google.cloud.bigtable.mirroring.hbase2_x.utils.AsyncRequestScheduling.reserveFlowControlResourcesThenScheduleSecondary;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.ResourceReservation;
import com.google.common.util.concurrent.FutureCallback;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

public class TestAsyncRequestScheduling {
  @Test
  public void testExceptionalPrimaryFuture() throws ExecutionException, InterruptedException {
    // We want to test that when primary database fails and returns an exceptional future,
    // no request is sent to secondary database and FlowController is appropriately dealt with.

    CompletableFuture<Void> exceptionalPrimaryFuture = new CompletableFuture<>();
    IOException ioe = new IOException("expected");
    exceptionalPrimaryFuture.completeExceptionally(ioe);

    ResourceReservation resourceReservation = mock(ResourceReservation.class);
    CompletableFuture<ResourceReservation> resourceReservationFuture =
        CompletableFuture.completedFuture(resourceReservation);

    Supplier<CompletableFuture<Void>> secondaryFutureSupplier = mock(Supplier.class);
    Function<Void, FutureCallback<Void>> verificationCreator = mock(Function.class);
    Consumer<Throwable> flowControlReservationErrorHandler = mock(Consumer.class);

    AsyncRequestScheduling.OperationStages<CompletableFuture<Void>> result =
        reserveFlowControlResourcesThenScheduleSecondary(
            exceptionalPrimaryFuture,
            resourceReservationFuture,
            secondaryFutureSupplier,
            verificationCreator,
            flowControlReservationErrorHandler);

    // reserveFlowControlResourcesThenScheduleSecondary() returns a pair of futures:
    // - verificationCompleted: completed with null when secondary request
    //   and its verification are finished. It's never completed exceptionally,
    // - userNotified: completed with primary request value or exception
    //   after receiving a ResourceReservation from FlowController.

    // We make sure that verificationCompleted is completed as expected.
    result.getVerificationCompletedFuture().get();

    // We make sure that userNotified passes the primary result through.
    // Note that CompletableFuture#get() wraps the exception in an ExecutionException.
    Exception primaryException = assertThrows(ExecutionException.class, result.userNotified::get);
    assertThat(primaryException.getCause()).isEqualTo(ioe);

    // resourceReservationFuture is a normally completed future so
    // flowControlReservationErrorHandler was never called.
    verify(flowControlReservationErrorHandler, never()).accept(nullable(Throwable.class));
    // The obtained resources must be released.
    verify(resourceReservation, times(1)).release();

    // Primary request failed, so there was no secondary request nor verification.
    verify(secondaryFutureSupplier, never()).get();
    verify(verificationCreator, never()).apply(nullable(Void.class));
  }

  @Test
  public void testExceptionalReservationFuture() throws ExecutionException, InterruptedException {
    // reserveFlowControlResourcesThenScheduleSecondary() returns a pair of futures:
    // - verificationCompleted: completed with null when secondary request
    //   and its verification are finished. It's never completed exceptionally,
    // - userNotified: completed with primary request value or exception
    //   after receiving a ResourceReservation from FlowController.

    // We want to test that when FlowController fails and returns an exceptional future,
    // both of the result futures are completed as expected.

    Result primaryResult = Result.create(new Cell[0]);
    CompletableFuture<Result> primaryFuture = CompletableFuture.completedFuture(primaryResult);
    CompletableFuture<ResourceReservation> exceptionalReservationFuture = new CompletableFuture<>();
    IOException ioe = new IOException("expected");
    exceptionalReservationFuture.completeExceptionally(ioe);

    Supplier<CompletableFuture<Result>> secondaryFutureSupplier = mock(Supplier.class);
    Function<Result, FutureCallback<Result>> verificationCreator = mock(Function.class);
    Consumer<Throwable> flowControlReservationErrorHandler = mock(Consumer.class);

    AsyncRequestScheduling.OperationStages<CompletableFuture<Result>> result =
        reserveFlowControlResourcesThenScheduleSecondary(
            primaryFuture,
            exceptionalReservationFuture,
            secondaryFutureSupplier,
            verificationCreator,
            flowControlReservationErrorHandler);

    // We make sure that userNotified passes the primary result through.
    assertThat(result.userNotified.get()).isEqualTo(primaryResult);
    // We make sure that verificationCompleted is completed normally.
    result.getVerificationCompletedFuture().get();

    // FlowController failed so the appropriate error handler was called.
    verify(flowControlReservationErrorHandler, times(1)).accept(any(Throwable.class));

    // FlowController failed, so there was no secondary request nor verification.
    verify(verificationCreator, never()).apply(nullable(Result.class));
    verify(secondaryFutureSupplier, never()).get();
  }
}
