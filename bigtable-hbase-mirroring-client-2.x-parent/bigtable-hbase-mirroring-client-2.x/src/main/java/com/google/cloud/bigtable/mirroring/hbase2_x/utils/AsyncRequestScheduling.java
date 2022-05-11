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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureUtils;
import com.google.common.util.concurrent.FutureCallback;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class AsyncRequestScheduling {
  public static <T>
      OperationStages<CompletableFuture<T>> reserveFlowControlResourcesThenScheduleSecondary(
          final CompletableFuture<T> primaryFuture,
          final CompletableFuture<FlowController.ResourceReservation> reservationFuture,
          final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
          final Function<T, FutureCallback<T>> verificationCreator) {
    return reserveFlowControlResourcesThenScheduleSecondary(
        primaryFuture, reservationFuture, secondaryFutureSupplier, verificationCreator, (e) -> {});
  }

  public static <T>
      OperationStages<CompletableFuture<T>> reserveFlowControlResourcesThenScheduleSecondary(
          final CompletableFuture<T> primaryFuture,
          final CompletableFuture<FlowController.ResourceReservation> reservationFuture,
          final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
          final Function<T, FutureCallback<T>> verificationCreator,
          final Consumer<Throwable> flowControlReservationErrorHandler) {
    OperationStages<CompletableFuture<T>> returnedValue =
        new OperationStages<>(new CompletableFuture<>());
    primaryFuture
        .thenAccept(
            (primaryResult) ->
                reservationFuture
                    .whenComplete(
                        (ignoredResult, ignoredError) ->
                            returnedValue.userNotified.complete(primaryResult))
                    .thenAccept(
                        reservation ->
                            FutureUtils.forwardResult(
                                scheduleVerificationAfterSecondaryOperation(
                                    reservation,
                                    secondaryFutureSupplier.get(),
                                    verificationCreator.apply(primaryResult)),
                                returnedValue.getVerificationCompletedFuture()))
                    .exceptionally(
                        reservationError -> {
                          flowControlReservationErrorHandler.accept(reservationError);
                          returnedValue.verificationCompleted();
                          throw new CompletionException(reservationError);
                        }))
        .exceptionally(
            primaryError -> {
              FlowController.cancelRequest(reservationFuture);
              returnedValue.userNotified.completeExceptionally(
                  FutureUtils.unwrapCompletionException(primaryError));
              returnedValue.verificationCompleted();
              throw new CompletionException(primaryError);
            });

    return returnedValue;
  }

  private static <T> CompletableFuture<Void> scheduleVerificationAfterSecondaryOperation(
      final FlowController.ResourceReservation reservation,
      final CompletableFuture<T> secondaryFuture,
      final FutureCallback<T> verificationCallback) {
    return secondaryFuture.handle(
        (secondaryResult, secondaryError) -> {
          try {
            if (secondaryError != null) {
              verificationCallback.onFailure(secondaryError);
            } else {
              verificationCallback.onSuccess(secondaryResult);
            }
          } finally {
            reservation.release();
          }
          return null;
        });
  }

  /**
   * Objects of this class gather futures which are completed on different stages of operation
   * execution.
   *
   * <p>These objects are used internally to register continuations to those events (e.g. complete
   * the future handed to the user or release resources after verification is done).
   *
   * @param <T> the type representing the stage of notifying the user; usually it's simply {@link
   *     CompletableFuture}{@literal <U>}, but in case of batch requests it is {@link List}<{@link
   *     CompletableFuture}{@literal <U>}>.
   */
  public static class OperationStages<T> {
    /**
     * The value the user is interested in. Usually the generic type is simply {@link
     * CompletableFuture}{@literal <U>}, but in case of batch requests it is {@link List}<{@link
     * CompletableFuture}{@literal <U>}>.
     */
    public final T userNotified;

    /**
     * It completes when background actions related to {@link OperationStages#userNotified} (e.g.
     * request to secondary database and result verification) are completed.
     */
    private final CompletableFuture<Void> verificationComplete;

    public OperationStages(T userNotified) {
      this.userNotified = userNotified;
      this.verificationComplete = new CompletableFuture<>();
    }

    public void verificationCompleted() {
      this.verificationComplete.complete(null);
    }

    public CompletableFuture<Void> getVerificationCompletedFuture() {
      return this.verificationComplete;
    }
  }
}
