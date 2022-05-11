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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Atomic Reference Counter that resolves internal future when all references were released.
 * Interested parties can obtain the future using {@link
 * ListenableReferenceCounter#getOnLastReferenceClosed()} and register listeners using {@link
 * ListenableFuture#addListener(Runnable, Executor)}.
 *
 * <p>Used to count references to tables and scanners used asynchronously in order to prevent
 * closing these resources while they have some scheduled or ongoing asynchronous operations.
 */
@InternalApi
public class ListenableReferenceCounter {
  private AtomicInteger referenceCount;
  private SettableFuture<Void> onLastReferenceClosed;

  public ListenableReferenceCounter() {
    this.referenceCount = new AtomicInteger(1);
    this.onLastReferenceClosed = SettableFuture.create();
  }

  public void incrementReferenceCount() {
    this.referenceCount.incrementAndGet();
  }

  public void decrementReferenceCount() {
    if (this.referenceCount.decrementAndGet() == 0) {
      this.onLastReferenceClosed.set(null);
    }
  }

  public ListenableFuture<Void> getOnLastReferenceClosed() {
    return this.onLastReferenceClosed;
  }

  /** Increments the reference counter and decrements it after the future is resolved. */
  public void holdReferenceUntilCompletion(ListenableFuture<?> future) {
    this.incrementReferenceCount();
    future.addListener(
        new Runnable() {
          @Override
          public void run() {
            ListenableReferenceCounter.this.decrementReferenceCount();
          }
        },
        MoreExecutors.directExecutor());
  }

  /** Increments the reference counter and decrements it after the provided object is closed. */
  public void holdReferenceUntilClosing(ListenableCloseable listenableCloseable) {
    this.incrementReferenceCount();
    listenableCloseable.addOnCloseListener(
        new Runnable() {
          @Override
          public void run() {
            ListenableReferenceCounter.this.decrementReferenceCount();
          }
        });
  }
}
