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
package com.google.cloud.bigtable.mirroring.core.utils;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.ResourceReservation;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.common.Scope;
import java.util.concurrent.ExecutionException;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * Static helper methods used for scheduling secondary database requests and results verification.
 */
@InternalApi("For internal usage only")
public class RequestScheduling {
  private static final Logger Log = new Logger(RequestScheduling.class);

  /**
   * This method starts supplied asynchronous operation after obtaining resource reservation from
   * the flow controller and registers a callback to be run after the operation is finished. If the
   * flow controller rejects the resource reservation or waiting for the reservation is interrupted,
   * the operation is not started and user-provided {@code flowControlReservationErrorConsumer} is
   * invoked.
   *
   * @param requestResourcesDescription Description of resources that should be reserved from the
   *     flow controller.
   * @param operation Asynchronous operation to start after obtaining resources.
   * @param callback Callback to be called after {@code operation} completes.
   * @param flowController Flow controller that should reserve the resources.
   * @param mirroringTracer Tracer used for tracing flow control and callback operations.
   * @param flowControlReservationErrorConsumer Handler that should be called if obtaining the
   *     reservation from the flow controller fails.
   * @return Future that will be set when the operation and callback scheduled by this operation
   *     finish running. The future will also be set if the flow controller rejects reservation
   *     request.
   */
  public static <T> ListenableFuture<Void> scheduleRequestWithCallback(
      final RequestResourcesDescription requestResourcesDescription,
      final Supplier<ListenableFuture<T>> operation,
      final FutureCallback<T> callback,
      final FlowController flowController,
      final MirroringTracer mirroringTracer,
      final Function<Throwable, Void> flowControlReservationErrorConsumer) {
    final SettableFuture<Void> callbackCompletedFuture = SettableFuture.create();

    final ListenableFuture<ResourceReservation> reservationRequest =
        flowController.asyncRequestResource(requestResourcesDescription);

    final ResourceReservation reservation =
        waitForReservation(
            reservationRequest, flowControlReservationErrorConsumer, mirroringTracer);

    if (reservation == null) {
      callbackCompletedFuture.set(null);
      return callbackCompletedFuture;
    }

    // Creates a callback that will release the reservation and set `callbackCompletedFuture` after
    // callback is finished.
    FutureCallback<? super T> wrappedCallback =
        wrapCallbackWithReleasingReservationAndCompletingFuture(
            callback, reservation, callbackCompletedFuture, mirroringTracer);

    // Start the asynchronous operation.
    ListenableFuture<T> operationsResult = operation.get();

    Futures.addCallback(operationsResult, wrappedCallback, MoreExecutors.directExecutor());

    return callbackCompletedFuture;
  }

  private static <T>
      FutureCallback<? super T> wrapCallbackWithReleasingReservationAndCompletingFuture(
          final FutureCallback<T> callback,
          final ResourceReservation reservation,
          final SettableFuture<Void> verificationCompletedFuture,
          MirroringTracer mirroringTracer) {
    return mirroringTracer.spanFactory.wrapWithCurrentSpan(
        new FutureCallback<T>() {
          @Override
          public void onSuccess(@NullableDecl T t) {
            try {
              Log.trace("starting verification %s", t);
              callback.onSuccess(t);
              Log.trace("verification done %s", t);
            } finally {
              reservation.release();
              verificationCompletedFuture.set(null);
            }
          }

          @Override
          public void onFailure(Throwable throwable) {
            try {
              callback.onFailure(throwable);
            } finally {
              reservation.release();
              verificationCompletedFuture.set(null);
            }
          }
        });
  }

  /**
   * Waits until reservation is obtained, rejected or interrupted.
   *
   * @return Obtained {@link ResourceReservation} or {@code null} in case of rejection or
   *     interruption.
   */
  private static ResourceReservation waitForReservation(
      ListenableFuture<ResourceReservation> reservationRequest,
      Function<Throwable, Void> flowControlReservationErrorConsumer,
      MirroringTracer mirroringTracer) {
    try {
      try (Scope scope = mirroringTracer.spanFactory.flowControlScope()) {
        return reservationRequest.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        flowControlReservationErrorConsumer.apply(e);
      } else {
        flowControlReservationErrorConsumer.apply(e.getCause());
      }
      FlowController.cancelRequest(reservationRequest);

      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return null;
    }
  }
}
