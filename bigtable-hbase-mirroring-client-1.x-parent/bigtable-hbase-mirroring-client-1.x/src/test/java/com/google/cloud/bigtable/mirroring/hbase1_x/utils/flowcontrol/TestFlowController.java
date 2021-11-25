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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestFlowController {
  static class Ledger implements SingleQueueFlowControlStrategy.Ledger {
    public int numRequestInFlight = 0;
    public int canAcquireResourcesCallsCount = 0;
    public int maxInFlightRequests = 0;
    public final int limit;
    public List<RequestResourcesDescription> acquireOrdering = new ArrayList<>();
    public SettableFuture<Void> futureToNotifyWhenCanAcquireResourceIsCalled = null;

    Ledger(int limit) {
      this.limit = limit;
    }

    public boolean canAcquireResource(RequestResourcesDescription resource) {
      if (this.futureToNotifyWhenCanAcquireResourceIsCalled != null) {
        this.futureToNotifyWhenCanAcquireResourceIsCalled.set(null);
      }
      this.canAcquireResourcesCallsCount += 1;
      return this.numRequestInFlight < this.limit;
    }

    @Override
    public boolean tryAcquireResource(RequestResourcesDescription resource) {
      if (this.canAcquireResource(resource)) {
        this.acquireOrdering.add(resource);
        this.numRequestInFlight += 1;
        this.maxInFlightRequests = Math.max(this.maxInFlightRequests, this.numRequestInFlight);
        return true;
      }
      return false;
    }

    @Override
    public void accountReleasedResources(RequestResourcesDescription resource) {
      this.numRequestInFlight -= 1;
    }
  }

  @Test
  public void testLockingAndUnlockingThreads()
      throws ExecutionException, InterruptedException, TimeoutException {

    // Mutex
    Ledger ledger = new Ledger(1);
    FlowControlStrategy flowControlStrategy = new SingleQueueFlowControlStrategy(ledger);
    final FlowController fc = new FlowController(flowControlStrategy);

    final SettableFuture<Void> threadStarted = SettableFuture.create();
    final SettableFuture<Void> threadEnded = SettableFuture.create();

    RequestResourcesDescription description = createRequest(1);
    ResourceReservation resourceReservation = fc.asyncRequestResource(description).get();

    int canAcquireCalls = ledger.canAcquireResourcesCallsCount;

    Thread thread =
        new Thread() {
          @Override
          public void run() {
            threadStarted.set(null);
            try {
              fc.asyncRequestResource(createRequest(1)).get();
            } catch (InterruptedException | ExecutionException e) {
              return;
            }
            threadEnded.set(null);
          }
        };

    thread.setDaemon(true);
    thread.start();

    threadStarted.get(3, TimeUnit.SECONDS);
    Thread.sleep(300);
    assertThat(threadEnded.isDone()).isFalse();
    assertThat(ledger.canAcquireResourcesCallsCount).isEqualTo(canAcquireCalls + 1);

    resourceReservation.release();
    threadEnded.get(3, TimeUnit.SECONDS);
    assertThat(ledger.canAcquireResourcesCallsCount).isEqualTo(canAcquireCalls + 2);
  }

  private RequestResourcesDescription createRequest(int size) {
    return new RequestResourcesDescription(new boolean[size]);
  }

  @Test
  public void testLockingAndUnlockingOrdering()
      throws ExecutionException, InterruptedException, TimeoutException {

    Ledger ledger = new Ledger(2);
    FlowControlStrategy flowControlStrategy = new SingleQueueFlowControlStrategy(ledger);
    final FlowController fc = new FlowController(flowControlStrategy);

    RequestResourcesDescription description1 = createRequest(2);
    RequestResourcesDescription description2 = createRequest(1);

    // Critical section is full.
    ResourceReservation reservation1 = fc.asyncRequestResource(description1).get();
    ResourceReservation reservation2 = fc.asyncRequestResource(description2).get();

    final int numThreads = 1000;
    List<Thread> threads = new ArrayList<>();
    for (int threadId = 0; threadId < numThreads; threadId++) {
      final SettableFuture<Void> threadStarted = SettableFuture.create();
      ledger.futureToNotifyWhenCanAcquireResourceIsCalled = threadStarted;

      final int finalThreadId = threadId;
      Thread thread =
          new Thread() {
            @Override
            public void run() {
              try {
                RequestResourcesDescription r = createRequest(finalThreadId);
                fc.asyncRequestResource(r).get().release();
              } catch (InterruptedException | ExecutionException ignored) {
              }
            }
          };

      thread.setDaemon(true);
      thread.start();
      threads.add(thread);

      // Wait until `fc.acquire` is called before starting next thread to ensure ordering.
      threadStarted.get(3, TimeUnit.SECONDS);
    }

    reservation1.release();
    reservation2.release();

    for (Thread t : threads) {
      t.join();
    }

    assertThat(ledger.acquireOrdering).hasSize(numThreads + 2); // + 2 initial entries
    assertThat(ledger.maxInFlightRequests).isEqualTo(2);

    for (int i = 0; i < ledger.acquireOrdering.size(); i++) {
      RequestResourcesDescription d = ledger.acquireOrdering.get(i);
      int expectedValue = Math.abs(i - 2);
      assertThat(d.numberOfResults).isEqualTo(expectedValue);
    }
  }

  @Test
  public void testCancelledReservationFutureIsRemovedFromFlowControllerWaitersList()
      throws ExecutionException, InterruptedException, TimeoutException {
    Ledger ledger = new Ledger(1);
    FlowControlStrategy flowControlStrategy = new SingleQueueFlowControlStrategy(ledger);
    final FlowController fc = new FlowController(flowControlStrategy);

    RequestResourcesDescription description = createRequest(1);
    ResourceReservation resourceReservation =
        fc.asyncRequestResource(description).get(); // critical section is full

    ListenableFuture<ResourceReservation> reservationFuture1 =
        fc.asyncRequestResource(createRequest(1));

    ListenableFuture<ResourceReservation> reservationFuture2 =
        fc.asyncRequestResource(createRequest(1));

    // Current thread is in critical section, future is first in the queue, thread is second.
    assertThat(reservationFuture1.cancel(true)).isTrue();

    Thread.sleep(300);
    assertThat(ledger.maxInFlightRequests).isEqualTo(1);

    // Releasing the resource should allow second future.
    resourceReservation.release();
    reservationFuture2.get(3, TimeUnit.SECONDS);
  }

  @Test
  public void testCancellingGrantedReservationIsNotSuccessful()
      throws ExecutionException, InterruptedException {
    Ledger ledger = new Ledger(1);
    FlowControlStrategy flowControlStrategy = new SingleQueueFlowControlStrategy(ledger);
    final FlowController fc = new FlowController(flowControlStrategy);

    ListenableFuture<ResourceReservation> reservationFuture1 =
        fc.asyncRequestResource(createRequest(1));
    assertThat(reservationFuture1.cancel(true)).isFalse();
    assertThat(reservationFuture1.isDone());
    reservationFuture1.get().release();
  }

  @Test
  public void testCancellingGrantedReservationFuture() {
    ResourceReservation reservation = mock(ResourceReservation.class);
    SettableFuture<ResourceReservation> grantedFuture = SettableFuture.create();
    grantedFuture.set(reservation);

    FlowController.cancelRequest(grantedFuture);
    verify(reservation, times(1)).release();
  }

  @Test
  public void testCancellingPendingReservationFuture() {
    ResourceReservation reservation = mock(ResourceReservation.class);
    SettableFuture<ResourceReservation> grantedFuture = SettableFuture.create();

    FlowController.cancelRequest(grantedFuture);
    verify(reservation, never()).release();
  }

  @Test
  public void testCancellingRejectedReservationFuture() {
    ResourceReservation reservation = mock(ResourceReservation.class);
    SettableFuture<ResourceReservation> notGrantedFuture = SettableFuture.create();
    notGrantedFuture.setException(new Exception("test"));

    FlowController.cancelRequest(notGrantedFuture);
    verify(reservation, never()).release();
  }
}
