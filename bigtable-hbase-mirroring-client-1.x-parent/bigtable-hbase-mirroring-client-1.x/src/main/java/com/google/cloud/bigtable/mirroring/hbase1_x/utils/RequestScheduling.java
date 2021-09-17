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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ExecutionException;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * Static helper methods used for scheduling secondary database requests and results verification.
 */
@InternalApi("For internal usage only")
public class RequestScheduling {
  private static final Logger Log = new Logger(RequestScheduling.class);

  public static <T> ListenableFuture<Void> scheduleVerificationAndRequestWithFlowControl(
      final RequestResourcesDescription requestResourcesDescription,
      final Supplier<ListenableFuture<T>> secondaryResultFutureSupplier,
      final FutureCallback<T> verificationCallback,
      final FlowController flowController) {
    final SettableFuture<Void> verificationCompletedFuture = SettableFuture.create();

    final ListenableFuture<ResourceReservation> reservationRequest =
        flowController.asyncRequestResource(requestResourcesDescription);
    try {
      final ResourceReservation reservation = reservationRequest.get();
      Futures.addCallback(
          secondaryResultFutureSupplier.get(),
          new FutureCallback<T>() {
            @Override
            public void onSuccess(@NullableDecl T t) {
              try {
                Log.trace("starting verification %s", t);
                verificationCallback.onSuccess(t);
                Log.trace("verification done %s", t);
              } finally {
                reservation.release();
                verificationCompletedFuture.set(null);
              }
            }

            @Override
            public void onFailure(Throwable throwable) {
              try {
                verificationCallback.onFailure(throwable);
              } finally {
                reservation.release();
                verificationCompletedFuture.set(null);
              }
            }
          },
          MoreExecutors.directExecutor());
    } catch (InterruptedException e) {
      if (!reservationRequest.cancel(true)) {
        try {
          reservationRequest.get().release();
        } catch (InterruptedException | ExecutionException ex) {
          // If we couldn't cancel the request, it must have already been set, we assume that we
          // will get the reservation without problems
          assert false;
        }
      }
      verificationCompletedFuture.set(null);
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      // We couldn't obtain reservation, this shouldn't happen.
      assert false;
      verificationCompletedFuture.set(null);
    }
    return verificationCompletedFuture;
  }
}
