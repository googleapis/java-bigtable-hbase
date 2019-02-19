/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.util;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class ApiFutureUtil {

  public static <T> ListenableFuture<T> adapt(final ApiFuture<T> apiFuture) {
    return new ListenableFuture<T>(){

      @Override public boolean cancel(boolean b) {
        return apiFuture.cancel(b);
      }

      @Override public boolean isCancelled() {
        return apiFuture.isCancelled();
      }

      @Override public boolean isDone() {
        return apiFuture.isDone();
      }

      @Override public T get() throws InterruptedException, ExecutionException {
        return apiFuture.get();
      }

      @Override public T get(long l, TimeUnit timeUnit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return apiFuture.get(l, timeUnit);
      }

      @Override public void addListener(Runnable listener, Executor executor) {
        apiFuture.addListener(listener, executor);
      }
    };
  }

  public static <T> ApiFuture<T> adapt(final ListenableFuture<T> listenableFuture) {
    return new ApiFuture<T>(){

      @Override public boolean cancel(boolean b) {
        return listenableFuture.cancel(b);
      }

      @Override public boolean isCancelled() {
        return listenableFuture.isCancelled();
      }

      @Override public boolean isDone() {
        return listenableFuture.isDone();
      }

      @Override public T get() throws InterruptedException, ExecutionException {
        return listenableFuture.get();
      }

      @Override public T get(long l, TimeUnit timeUnit)
          throws InterruptedException, ExecutionException, TimeoutException {
        return listenableFuture.get(l, timeUnit);
      }

      @Override public void addListener(Runnable listener, Executor executor) {
        listenableFuture.addListener(listener, executor);
      }
    };
  }
}
