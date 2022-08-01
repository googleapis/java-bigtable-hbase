/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class PropagatingThread extends Thread {
  private SettableFuture<Void> resultFuture = SettableFuture.create();

  @Override
  public final void run() {
    try {
      this.performTask();
      resultFuture.set(null);
    } catch (Throwable t) {
      resultFuture.setException(t);
    }
  }

  public abstract void performTask() throws Throwable;

  public void propagatingJoin() {
    try {
      this.resultFuture.get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void propagatingJoin(long millis) throws TimeoutException {
    try {
      this.resultFuture.get(millis, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
