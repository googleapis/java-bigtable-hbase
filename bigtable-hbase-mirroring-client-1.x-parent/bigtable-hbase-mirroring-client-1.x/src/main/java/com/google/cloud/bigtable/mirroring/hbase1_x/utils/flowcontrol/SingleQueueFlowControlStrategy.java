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
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.AcquiredResourceReservation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * A {@link FlowControlStrategy} that keeps a queue of requests and admits then in order of
 * appearance.
 */
@InternalApi("For internal usage only")
public class SingleQueueFlowControlStrategy implements FlowControlStrategy {
  // Used to prevent starving big requests by a lot of smaller ones.
  private final Queue<AcquiredResourceReservation> waitingRequestsQueue = new ArrayDeque<>();
  // Counts in-flight requests and decides if new requests can be allowed.
  private final Ledger ledger;

  protected SingleQueueFlowControlStrategy(Ledger ledger) {
    this.ledger = ledger;
  }

  @Override
  public ListenableFuture<ResourceReservation> asyncRequestResourceReservation(
      RequestResourcesDescription resourcesDescription) {
    AcquiredResourceReservation resources =
        new AcquiredResourceReservation(resourcesDescription, this);

    // We shouldn't complete futures with the lock held, so we use this list to gather those which
    // should be completed once we release the lock
    List<AcquiredResourceReservation> resourcesToBeNotified;
    synchronized (this) {
      this.waitingRequestsQueue.add(resources);
      resourcesToBeNotified = this.allowWaiters();
    }
    notifyWaiters(resourcesToBeNotified);

    return resources.notification;
  }

  @Override
  public final void releaseResource(RequestResourcesDescription resource) {
    // We shouldn't complete futures with the lock held, so we use this list to gather those which
    // should be completed once we release the lock
    List<AcquiredResourceReservation> resourcesToBeNotified;
    synchronized (this) {
      this.ledger.accountReleasedResources(resource);
      resourcesToBeNotified = this.allowWaiters();
    }
    notifyWaiters(resourcesToBeNotified);
  }

  private synchronized List<AcquiredResourceReservation> allowWaiters() {
    List<AcquiredResourceReservation> resourcesToBeNotified = new ArrayList<>();

    while (!this.waitingRequestsQueue.isEmpty()
        && this.tryAcquireResource(this.waitingRequestsQueue.peek().requestResourcesDescription)) {
      AcquiredResourceReservation reservation = this.waitingRequestsQueue.remove();
      resourcesToBeNotified.add(reservation);
    }

    return resourcesToBeNotified;
  }

  private static void notifyWaiters(List<AcquiredResourceReservation> resourcesToBeNotified) {
    for (AcquiredResourceReservation reservation : resourcesToBeNotified) {
      reservation.notifyWaiter();
    }
  }

  public boolean tryAcquireResource(RequestResourcesDescription requestResourcesDescription) {
    boolean canAcquire = this.ledger.canAcquireResource(requestResourcesDescription);
    if (canAcquire) {
      this.ledger.accountAcquiredResource(requestResourcesDescription);
    }
    return canAcquire;
  }

  interface Ledger {
    boolean canAcquireResource(RequestResourcesDescription requestResourcesDescription);

    void accountAcquiredResource(RequestResourcesDescription requestResourcesDescription);

    void accountReleasedResources(RequestResourcesDescription resource);
  }
}
