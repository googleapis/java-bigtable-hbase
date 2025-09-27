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

package com.google.cloud.bigtable.mirroring.core;


import static com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounterUtils.holdReferenceUntilCompletion;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.utils.RequestScheduling;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Helper class that holds common parameters to {@link
 * RequestScheduling#scheduleRequestWithCallback(RequestResourcesDescription, Supplier,
 * FutureCallback, FlowController, MirroringTracer, Function)}.
 *
 * <p>It also takes care of reference counting all scheduled operations.
 */
@InternalApi("For internal usage only")
public class RequestScheduler {
  final FlowController flowController;
  final MirroringTracer mirroringTracer;
  final ReferenceCounter referenceCounter;

  public RequestScheduler(
      FlowController flowController,
      MirroringTracer mirroringTracer,
      ReferenceCounter referenceCounter) {
    this.flowController = flowController;
    this.mirroringTracer = mirroringTracer;
    this.referenceCounter = referenceCounter;
  }

  public RequestScheduler withReferenceCounter(ReferenceCounter referenceCounter) {
    return new RequestScheduler(this.flowController, this.mirroringTracer, referenceCounter);
  }

  public <T> ListenableFuture<Void> scheduleRequestWithCallback(
      final RequestResourcesDescription requestResourcesDescription,
      final Supplier<ListenableFuture<T>> secondaryResultFutureSupplier,
      final FutureCallback<T> verificationCallback) {
    return this.scheduleRequestWithCallback(
        requestResourcesDescription,
        secondaryResultFutureSupplier,
        verificationCallback,
        // noop flowControlReservationErrorConsumer
        new Function<Throwable, Void>() {
          @Override
          public Void apply(Throwable t) {
            return null;
          }
        });
  }

  public <T> ListenableFuture<Void> scheduleRequestWithCallback(
      final RequestResourcesDescription requestResourcesDescription,
      final Supplier<ListenableFuture<T>> secondaryResultFutureSupplier,
      final FutureCallback<T> verificationCallback,
      final Function<Throwable, Void> flowControlReservationErrorConsumer) {
    ListenableFuture<Void> future =
        RequestScheduling.scheduleRequestWithCallback(
            requestResourcesDescription,
            secondaryResultFutureSupplier,
            verificationCallback,
            this.flowController,
            this.mirroringTracer,
            flowControlReservationErrorConsumer);
    holdReferenceUntilCompletion(this.referenceCounter, future);
    return future;
  }
}