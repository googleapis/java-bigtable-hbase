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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  static class SingleQueueTestLedger implements SingleQueueFlowControlStrategy.Ledger {
    public int numRequestInFlight = 0;
    public int canAcquireResourcesCallsCount = 0;
    public int maxInFlightRequests = 0;
    public final int limitInFlightRequests;
    public List<RequestResourcesDescription> acquireOrdering = new ArrayList<>();
    public SettableFuture<Void> futureToNotifyWhenCanAcquireResourceIsCalled = null;

    SingleQueueTestLedger(int limitInFlightRequests) {
      this.limitInFlightRequests = limitInFlightRequests;
    }

    public boolean canAcquireResource(RequestResourcesDescription resource) {
      if (this.futureToNotifyWhenCanAcquireResourceIsCalled != null) {
        this.futureToNotifyWhenCanAcquireResourceIsCalled.set(null);
      }

      this.canAcquireResourcesCallsCount += 1;
      return this.numRequestInFlight < this.limitInFlightRequests;
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
    SingleQueueTestLedger testLedger = new SingleQueueTestLedger(1);
    FlowControlStrategy flowControlStrategy = new SingleQueueFlowControlStrategy(testLedger);
    final FlowController fc = new FlowController(flowControlStrategy);

    final SettableFuture<Void> threadStarted = SettableFuture.create();
    final SettableFuture<Void> threadEnded = SettableFuture.create();

    RequestResourcesDescription description = createRequest(1);
    ResourceReservation resourceReservation = fc.asyncRequestResource(description).get();
    assertThat(testLedger.canAcquireResourcesCallsCount).isEqualTo(1);

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
    assertThat(testLedger.canAcquireResourcesCallsCount).isEqualTo(2);

    resourceReservation.release();
    threadEnded.get(3, TimeUnit.SECONDS);
    assertThat(testLedger.canAcquireResourcesCallsCount).isEqualTo(3);
  }

  private RequestResourcesDescription createRequest(int size) {
    return new RequestResourcesDescription(new boolean[size]);
  }

  @Test
  public void testSingleQueueStrategyAllowsRequestsInOrder()
      throws ExecutionException, InterruptedException, TimeoutException {

    int limitRequestsInFlight = 2;
    SingleQueueTestLedger testLedger = new SingleQueueTestLedger(limitRequestsInFlight);
    FlowControlStrategy flowControlStrategy = new SingleQueueFlowControlStrategy(testLedger);
    final FlowController fc = new FlowController(flowControlStrategy);
    // This FlowController limits number of requests in flight and admits them in order.

    ResourceReservation reservation1 = fc.asyncRequestResource(createRequest(2)).get();
    ResourceReservation reservation2 = fc.asyncRequestResource(createRequest(1)).get();
    // Maximal number of requests in flight has been reached.

    final int numThreads = 1000;
    List<Thread> threads = new ArrayList<>();
    for (int threadId = 0; threadId < numThreads; threadId++) {
      final SettableFuture<Void> threadBlockedOnAcquiringResources = SettableFuture.create();
      testLedger.futureToNotifyWhenCanAcquireResourceIsCalled = threadBlockedOnAcquiringResources;

      final int finalThreadId = threadId;
      Thread thread =
          new Thread() {
            @Override
            public void run() {
              try {
                RequestResourcesDescription r = createRequest(finalThreadId);
                fc.asyncRequestResource(r).get().release();
              } catch (InterruptedException | ExecutionException ignored) {
                fail("shouldn't have thrown");
              }
            }
          };

      thread.setDaemon(true);
      thread.start();
      threads.add(thread);

      // We want to check that our threads are given resources in order they asked for them, so to
      // have a well-defined order we can only have one thread running before it blocks on future
      // received from FlowController.
      threadBlockedOnAcquiringResources.get(3, TimeUnit.SECONDS);
    }

    reservation1.release();
    reservation2.release();

    for (Thread t : threads) {
      t.join();
    }

    assertThat(testLedger.acquireOrdering).hasSize(numThreads + 2); // + 2 initial entries
    assertThat(testLedger.maxInFlightRequests).isEqualTo(limitRequestsInFlight);

    for (int i = 0; i < testLedger.acquireOrdering.size(); i++) {
      RequestResourcesDescription resourceDescriptor = testLedger.acquireOrdering.get(i);
      int expectedValue = Math.abs(i - 2);
      assertThat(resourceDescriptor.numberOfResults).isEqualTo(expectedValue);
    }
  }

  @Test
  public void testCancelledReservationFutureIsRemovedFromFlowControllerWaitersList()
      throws ExecutionException, InterruptedException, TimeoutException {
    SingleQueueTestLedger testLedger = new SingleQueueTestLedger(1);
    FlowControlStrategy flowControlStrategy = new SingleQueueFlowControlStrategy(testLedger);
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
    assertThat(testLedger.maxInFlightRequests).isEqualTo(1);

    // Releasing the resource should allow second future.
    resourceReservation.release();
    reservationFuture2.get(3, TimeUnit.SECONDS);
  }

  @Test
  public void testCancellingGrantedReservationIsNotSuccessful()
      throws ExecutionException, InterruptedException {
    SingleQueueTestLedger testLedger = new SingleQueueTestLedger(1);
    FlowControlStrategy flowControlStrategy = new SingleQueueFlowControlStrategy(testLedger);
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
  public void testCancellingPendingReservationFuture()
      throws ExecutionException, InterruptedException {
    ExecutionException flowControllerException =
        new ExecutionException(new Exception("FlowController rejected request"));

    ListenableFuture<ResourceReservation> pendingFuture = mock(ListenableFuture.class);
    when(pendingFuture.cancel(anyBoolean())).thenReturn(false);
    when(pendingFuture.get()).thenThrow(flowControllerException);

    FlowController.cancelRequest(pendingFuture);
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
