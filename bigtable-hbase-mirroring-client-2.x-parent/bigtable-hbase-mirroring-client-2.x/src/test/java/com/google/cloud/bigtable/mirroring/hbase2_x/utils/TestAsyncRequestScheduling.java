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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.common.util.concurrent.FutureCallback;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.Test;

public class TestAsyncRequestScheduling {
  @Test
  public void testExceptionalPrimaryFuture() throws ExecutionException, InterruptedException {
    CompletableFuture<Void> exceptionalFuture = new CompletableFuture<>();
    IOException ioe = new IOException("expected");
    exceptionalFuture.completeExceptionally(ioe);

    FlowController.ResourceReservation resourceReservation =
        mock(FlowController.ResourceReservation.class);
    CompletableFuture<FlowController.ResourceReservation> resourceReservationFuture =
        CompletableFuture.completedFuture(resourceReservation);

    Supplier<CompletableFuture<Void>> secondaryFutureSupplier = mock(Supplier.class);
    Function<Void, FutureCallback<Void>> verificationCreator = mock(Function.class);
    Consumer<Throwable> flowControlReservationErrorHandler = mock(Consumer.class);

    AsyncRequestScheduling.OperationStages<CompletableFuture<Void>> result =
        reserveFlowControlResourcesThenScheduleSecondary(
            exceptionalFuture,
            resourceReservationFuture,
            secondaryFutureSupplier,
            verificationCreator,
            flowControlReservationErrorHandler);

    result.getVerificationCompletedFuture().get();
    final List<Throwable> resultThrowableList = new ArrayList<>();
    result
        .userNotified
        .exceptionally(
            t -> {
              resultThrowableList.add(t);
              return null;
            })
        .get();
    assertThat(resultThrowableList.size()).isEqualTo(1);
    assertThat(resultThrowableList.get(0)).isEqualTo(ioe);

    verify(resourceReservation, times(1)).release();
    verify(verificationCreator, never()).apply((Void) any());
    verify(secondaryFutureSupplier, never()).get();
    verify(flowControlReservationErrorHandler, never()).accept(any());

    assertThat(resourceReservationFuture.isCancelled());
  }

  @Test
  public void testExceptionalReservationFuture() throws ExecutionException, InterruptedException {
    CompletableFuture<Void> primaryFuture = CompletableFuture.completedFuture(null);
    CompletableFuture<FlowController.ResourceReservation> exceptionalFuture =
        new CompletableFuture<>();
    IOException ioe = new IOException("expected");
    exceptionalFuture.completeExceptionally(ioe);

    Supplier<CompletableFuture<Void>> secondaryFutureSupplier = mock(Supplier.class);
    Function<Void, FutureCallback<Void>> verificationCreator = mock(Function.class);
    Consumer<Throwable> flowControlReservationErrorHandler = mock(Consumer.class);

    AsyncRequestScheduling.OperationStages<CompletableFuture<Void>> result =
        reserveFlowControlResourcesThenScheduleSecondary(
            primaryFuture,
            exceptionalFuture,
            secondaryFutureSupplier,
            verificationCreator,
            flowControlReservationErrorHandler);

    final List<Throwable> resultThrowableList = new ArrayList<>();
    result.userNotified.get();
    result.getVerificationCompletedFuture().get();

    verify(verificationCreator, never()).apply((Void) any());
    verify(secondaryFutureSupplier, never()).get();
    verify(flowControlReservationErrorHandler, times(1)).accept(any());
  }
}
