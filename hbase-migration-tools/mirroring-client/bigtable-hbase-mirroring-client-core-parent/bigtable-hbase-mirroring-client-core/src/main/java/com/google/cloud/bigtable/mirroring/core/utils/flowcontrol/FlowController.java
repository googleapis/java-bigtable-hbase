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
package com.google.cloud.bigtable.mirroring.core.utils.flowcontrol;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * FlowController limits resources (RAM, number of requests) used by asynchronous requests to
 * secondary database. It is used to keep track of all requests sent to secondary from {@link
 * com.google.cloud.bigtable.mirroring.core.MirroringTable} and {@link
 * com.google.cloud.bigtable.mirroring.core.bufferedmutator.MirroringBufferedMutator}, most of the
 * times called from a helper method {@link
 * com.google.cloud.bigtable.mirroring.core.utils.RequestScheduling#scheduleRequestWithCallback(
 * RequestResourcesDescription, Supplier, FutureCallback, FlowController, MirroringTracer,
 * Function)}. FlowController and {@link FlowControlStrategy} do not allocate any actual resources,
 * they are used for accounting the amount of resources used by other classes, thus we say that they
 * "reserve" resources rather than allocate them.
 *
 * <p>Call to {@link #asyncRequestResource(RequestResourcesDescription)} returns a future that will
 * be completed when {@link FlowControlStrategy} reserves requested amount of resources and the
 * requesting actor is allowed perform its operation. {@link ResourceReservation}s obtained this way
 * should be released using {@link ResourceReservation#release()} after the operation is completed.
 * The future might also be completed exceptionally if the {@link FlowControlStrategy} rejects a
 * request and resources for it won't be reserved. The future returned from {@link
 * #asyncRequestResource(RequestResourcesDescription)} can be cancelled (using {@link
 * #cancelRequest(Future)}) if the requesting actor is not longer willing to perform the request.
 * Each request should be released or have its future cancelled. Futures can be safely cancelled
 * even if they have been already completed - that would simply release reserved resources.
 *
 * <p>Requests are completed in order determined by {@link FlowControlStrategy}.
 *
 * <p>{@link FlowController} and {@link AcquiredResourceReservation} provide a simpler interface
 * over {@link FlowControlStrategy} - a {@link ResourceReservation#release()} called on {@link
 * ResourceReservation} obtained from an instance of FlowController will always release appropriate
 * amount of resources from correct {@link FlowControlStrategy}.
 *
 * <p>Thread-safe because uses thread-safe interface of {@link FlowControlStrategy}.
 */
@InternalApi("For internal usage only")
public class FlowController {
  private final FlowControlStrategy flowControlStrategy;

  public FlowController(FlowControlStrategy flowControlStrategy) {
    this.flowControlStrategy = flowControlStrategy;
  }

  public ListenableFuture<ResourceReservation> asyncRequestResource(
      RequestResourcesDescription resourcesDescription) {
    return this.flowControlStrategy.asyncRequestResourceReservation(resourcesDescription);
  }

  public static void cancelRequest(Future<ResourceReservation> resourceReservationFuture) {
    // The cancellation may fail if the resources were already allocated by the FlowController, then
    // we should free them, or when the reservation was rejected, which we should ignore.
    if (!resourceReservationFuture.cancel(true)) {
      // We cannot cancel the reservation future. This means that the future was already completed
      // by calling `set()` or `setException()`.
      try {
        resourceReservationFuture.get().release();
      } catch (InterruptedException ex) {
        // This shouldn't happen. The future was already set with `set()` or `setException()`, which
        // means that calling `.get()` on it shouldn't block.
        throw new IllegalStateException(
            "A reservation future couldn't be cancelled, but obtaining its result has thrown "
                + "InterruptedException. This is unexpected.",
            ex);
      } catch (ExecutionException ex) {
        // The request was rejected by flow controller (e.g. cancelled).
        // `AcquiredResourceReservation` handles such cases correctly and will release associated
        // resources.
      }
    }
  }

  /**
   * Default implementation of {@link ResourceReservation} that can be used by {@link
   * FlowControlStrategy} implementations as an entry to be notified when resources for request are
   * available.
   *
   * <p>Not thread-safe.
   */
  public static class AcquiredResourceReservation implements ResourceReservation {
    final RequestResourcesDescription requestResourcesDescription;
    final SettableFuture<ResourceReservation> notification;
    final FlowControlStrategy flowControlStrategy;
    private boolean released;
    private boolean notified;

    public AcquiredResourceReservation(
        RequestResourcesDescription requestResourcesDescription,
        SingleQueueFlowControlStrategy flowControlStrategy) {
      this.requestResourcesDescription = requestResourcesDescription;
      this.flowControlStrategy = flowControlStrategy;
      this.notification = SettableFuture.create();
      this.released = false;
      this.notified = false;
    }

    public void notifyWaiter() {
      Preconditions.checkState(!this.notified);
      this.notified = true;
      if (!this.notification.set(this)) {
        Preconditions.checkState(this.notification.isCancelled());
        // The notification was cancelled, we should release its resources.
        this.release();
      }
    }

    @Override
    public void release() {
      if (!this.released) {
        this.flowControlStrategy.releaseResource(this.requestResourcesDescription);
        this.released = true;
      }
    }
  }
}
