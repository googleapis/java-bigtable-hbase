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
package com.google.cloud.bigtable.mirroring.core.utils.referencecounting;

import static com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounterUtils.holdReferenceUntilCompletion;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestListenableReferenceCounter {
  @Test
  public void testFutureIsResolvedWhenCounterReachesZero() {
    Runnable runnable = mock(Runnable.class);

    ListenableReferenceCounter listenableReferenceCounter = new ListenableReferenceCounter();
    listenableReferenceCounter
        .getOnLastReferenceClosed()
        .addListener(runnable, MoreExecutors.directExecutor());

    verify(runnable, never()).run();
    listenableReferenceCounter.incrementReferenceCount(); // 2

    verify(runnable, never()).run();
    listenableReferenceCounter.decrementReferenceCount(); // 1

    verify(runnable, never()).run();
    listenableReferenceCounter.decrementReferenceCount(); // 0
    verify(runnable, times(1)).run();
  }

  @Test
  public void testCounterIsDecrementedWhenReferenceIsDone() {
    ListenableReferenceCounter listenableReferenceCounter = spy(new ListenableReferenceCounter());
    SettableFuture<Void> future = SettableFuture.create();
    verify(listenableReferenceCounter, never()).incrementReferenceCount();
    holdReferenceUntilCompletion(listenableReferenceCounter, future);
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, never()).decrementReferenceCount();
    future.set(null);
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, times(1)).decrementReferenceCount();
  }

  @Test
  public void testCounterIsDecrementedWhenReferenceThrowsException() {
    ListenableReferenceCounter listenableReferenceCounter = spy(new ListenableReferenceCounter());
    SettableFuture<Void> future = SettableFuture.create();
    verify(listenableReferenceCounter, never()).incrementReferenceCount();
    holdReferenceUntilCompletion(listenableReferenceCounter, future);
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, never()).decrementReferenceCount();
    future.setException(new Exception("expected"));
    verify(listenableReferenceCounter, times(1)).incrementReferenceCount();
    verify(listenableReferenceCounter, times(1)).decrementReferenceCount();
  }
}
