/*
 * Copyright 2017 Google LLC
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
package com.google.cloud.bigtable.hbase2_x;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.InternalApi;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.CompletableFuture;

/**
 * Utility methods for converting gax {@link ApiFuture} to {@link CompletableFuture}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ApiFutureUtils {
  public static <T> CompletableFuture<T> toCompletableFuture(ApiFuture<T> apiFuture) {
    CompletableFuture<T> completableFuture =
        new CompletableFuture<T>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            boolean result = apiFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    ApiFutureCallback<T> callback =
        new ApiFutureCallback<T>() {
          public void onFailure(Throwable throwable) {
            completableFuture.completeExceptionally(throwable);
          }

          public void onSuccess(T t) {
            completableFuture.complete(t);
          }
        };
    ApiFutures.addCallback(apiFuture, callback, MoreExecutors.directExecutor());

    return completableFuture;
  }

  public static <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }
}
