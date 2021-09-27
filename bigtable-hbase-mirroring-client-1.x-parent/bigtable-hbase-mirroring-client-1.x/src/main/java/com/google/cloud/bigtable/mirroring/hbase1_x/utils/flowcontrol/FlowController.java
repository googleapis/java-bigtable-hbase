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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol;

import com.google.api.core.InternalApi;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * FlowController limits the number of concurrently performed requests to the secondary database.
 * Call to {@link #asyncRequestResource(RequestResourcesDescription)} returns a future that will be
 * completed when {@link FlowControlStrategy} decides that it can be allowed to perform the
 * requests. The future might also be completed exceptionally if the resource was not allowed to
 * obtain the resources.
 *
 * <p>Order of allowing requests in determined by {@link FlowControlStrategy}.
 *
 * <p>Thread-safe.
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
      try {
        resourceReservationFuture.get().release();
      } catch (InterruptedException ex) {
        // If we couldn't cancel the request, it must have already been set, we assume
        // that we will get the reservation without problems
        assert false;
      } catch (ExecutionException ex) {
        // The request was rejected.
      }
    }
  }

  /**
   * Object describing resources acquired by {@link FlowController}. Users is responsible for
   * calling {@link #release()} when they no longer use acquired resources and they can be assigned
   * to other parties.
   */
  public interface ResourceReservation {
    void release();
  }

  /**
   * Default implementation of {@link ResourceReservation} that can be used by {@link
   * FlowControlStrategy} implementations as an entry to be notified when resources for request are
   * available.
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

    void notifyWaiter() {
      assert !this.notified;
      this.notified = true;
      if (!this.notification.set(this)) {
        assert this.notification.isCancelled();
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
