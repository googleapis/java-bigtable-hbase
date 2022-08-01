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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.cloud.bigtable.mirroring.core.MirroringOptions;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowControlStrategy;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.ResourceReservation;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.Executor;

public class BlockingFlowControllerStrategy implements FlowControlStrategy {

  private static SettableFuture<Void> unblock;

  public static void reset() {
    unblock = SettableFuture.create();
  }

  public static void unblock() {
    unblock.set(null);
  }

  @Override
  public ListenableFuture<ResourceReservation> asyncRequestResourceReservation(
      RequestResourcesDescription resourcesDescription) {
    final SettableFuture<ResourceReservation> reservation = SettableFuture.create();
    unblock.addListener(
        new Runnable() {
          @Override
          public void run() {
            reservation.set(
                new ResourceReservation() {
                  @Override
                  public void release() {}
                });
          }
        },
        new Executor() {
          @Override
          public void execute(Runnable runnable) {
            runnable.run();
          }
        });
    return reservation;
  }

  @Override
  public void releaseResource(RequestResourcesDescription resource) {}

  public static class Factory implements FlowControlStrategy.Factory {

    @Override
    public FlowControlStrategy create(MirroringOptions options) throws Throwable {
      return new BlockingFlowControllerStrategy();
    }
  }
}
