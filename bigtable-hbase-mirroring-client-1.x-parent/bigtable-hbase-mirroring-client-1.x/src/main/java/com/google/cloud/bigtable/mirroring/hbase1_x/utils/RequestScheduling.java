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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ExecutionException;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * Static helper methods used for scheduling secondary database requests and results verification.
 */
@InternalApi("For internal usage only")
public class RequestScheduling {
  public static <T> void scheduleVerificationAndRequestWithFlowControl(
      final RequestResourcesDescription requestResourcesDescription,
      final ListenableFuture<T> secondaryGetFuture,
      final FutureCallback<T> verificationCallback,
      final FlowController flowController) {
    final ListenableFuture<ResourceReservation> reservationRequest =
        flowController.asyncRequestResource(requestResourcesDescription);
    try {
      final ResourceReservation reservation = reservationRequest.get();
      Futures.addCallback(
          secondaryGetFuture,
          new FutureCallback<T>() {
            @Override
            public void onSuccess(@NullableDecl T t) {
              try {
                verificationCallback.onSuccess(t);
              } finally {
                reservation.release();
              }
            }

            @Override
            public void onFailure(Throwable throwable) {
              reservation.release();
              verificationCallback.onFailure(throwable);
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
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      // We couldn't obtain reservation, this shouldn't happen.
      assert false;
    }
  }
}
