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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.rules.ExternalResource;

public class ExecutorServiceRule extends ExternalResource {
  private enum Type {
    Fixed,
    Cached,
    Single,
  }

  public final int numThreads;
  public final Type type;
  public ExecutorService executorService;
  public final boolean spyed;

  private ExecutorServiceRule(Type type, int numThreads, boolean spyed) {
    this.type = type;
    this.numThreads = numThreads;
    this.spyed = spyed;
  }

  public static ExecutorServiceRule singleThreadedExecutor() {
    return new ExecutorServiceRule(Type.Single, 1, false);
  }

  public static ExecutorServiceRule spyedSingleThreadedExecutor() {
    return new ExecutorServiceRule(Type.Single, 1, true);
  }

  public static ExecutorServiceRule cachedPoolExecutor() {
    return new ExecutorServiceRule(Type.Cached, 0, false);
  }

  public static ExecutorServiceRule spyedCachedPoolExecutor() {
    return new ExecutorServiceRule(Type.Cached, 0, true);
  }

  public static ExecutorServiceRule fixedPoolExecutor(int numThreads) {
    Preconditions.checkArgument(numThreads > 0);
    return new ExecutorServiceRule(Type.Fixed, numThreads, false);
  }

  public static ExecutorServiceRule spyedFixedPoolExecutor(int numThreads) {
    Preconditions.checkArgument(numThreads > 0);
    return new ExecutorServiceRule(Type.Fixed, numThreads, true);
  }

  @Override
  protected void before() throws Throwable {
    super.before();
    ExecutorService executorService = createExecutor();
    if (this.spyed) {
      this.executorService = spy(MoreExecutors.listeningDecorator(executorService));
    } else {
      this.executorService = executorService;
    }
  }

  private ExecutorService createExecutor() {
    switch (this.type) {
      case Single:
        return Executors.newSingleThreadExecutor();
      case Cached:
        return Executors.newCachedThreadPool();
      case Fixed:
        return Executors.newFixedThreadPool(numThreads);
      default:
        throw new UnsupportedOperationException(
            String.format("Unknown ExecutorService type: %s", this.type));
    }
  }

  @Override
  protected void after() {
    super.after();
    this.executorService.shutdownNow();
  }

  public void waitForExecutor() {
    this.executorService.shutdown();
    try {
      if (!this.executorService.awaitTermination(3, TimeUnit.SECONDS)) {
        fail("executor did not terminate");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
