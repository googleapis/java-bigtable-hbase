/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures;

import com.google.cloud.bigtable.mirroring.hbase2_x.utils.AsyncRequestScheduling;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class FutureUtils {
  public static <T> void forwardResult(CompletableFuture<T> from, CompletableFuture<T> to) {
    from.whenComplete(
        (result, error) -> {
          if (error != null) {
            to.completeExceptionally(error);
          } else {
            to.complete(result);
          }
        });
  }

  public static <T> void forwardResult(
      AsyncRequestScheduling.OperationStages<CompletableFuture<T>> from,
      AsyncRequestScheduling.OperationStages<CompletableFuture<T>> to) {
    forwardResult(from.userNotified, to.userNotified);
    forwardResult(from.getVerificationCompletedFuture(), to.getVerificationCompletedFuture());
  }

  public static Throwable unwrapCompletionException(Throwable e) {
    if (e instanceof CompletionException) {
      return e.getCause();
    }
    return e;
  }
}
