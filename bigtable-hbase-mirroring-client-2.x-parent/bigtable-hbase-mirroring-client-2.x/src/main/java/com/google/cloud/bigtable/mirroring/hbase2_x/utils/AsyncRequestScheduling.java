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
import com.google.common.util.concurrent.FutureCallback;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

public class AsyncRequestScheduling {
  public static <T> CompletableFuture<T> reserveFlowControlResourcesThenScheduleSecondary(
      final CompletableFuture<T> primaryFuture,
      final CompletableFuture<FlowController.ResourceReservation> reservationFuture,
      final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
      final Function<T, FutureCallback<T>> verificationCreator) {
    return reserveFlowControlResourcesThenScheduleSecondary(
        primaryFuture, reservationFuture, secondaryFutureSupplier, verificationCreator, () -> {});
  }

  public static <T> CompletableFuture<T> reserveFlowControlResourcesThenScheduleSecondary(
      final CompletableFuture<T> primaryFuture,
      final CompletableFuture<FlowController.ResourceReservation> reservationFuture,
      final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
      final Function<T, FutureCallback<T>> verificationCreator,
      final Runnable flowControlReservationErrorHandler) {
    CompletableFuture<T> resultFuture = new CompletableFuture<T>();
    primaryFuture.whenComplete(
        (primaryResult, primaryError) -> {
          if (primaryError != null) {
            FlowController.cancelRequest(reservationFuture);
            resultFuture.completeExceptionally(primaryError);
            return;
          }
          reservationFuture.whenComplete(
              (reservation, reservationError) -> {
                resultFuture.complete(primaryResult);
                if (reservationError != null) {
                  flowControlReservationErrorHandler.run();
                  return;
                }

                scheduleVerificationAfterSecondaryOperation(
                    reservation,
                    secondaryFutureSupplier.get(),
                    verificationCreator.apply(primaryResult));
              });
        });
    return resultFuture;
  }

  private static <T> void scheduleVerificationAfterSecondaryOperation(
      final FlowController.ResourceReservation reservation,
      final CompletableFuture<T> secondaryFuture,
      final FutureCallback<T> verificationCallback) {
    secondaryFuture.whenComplete(
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
        });
  }
}
