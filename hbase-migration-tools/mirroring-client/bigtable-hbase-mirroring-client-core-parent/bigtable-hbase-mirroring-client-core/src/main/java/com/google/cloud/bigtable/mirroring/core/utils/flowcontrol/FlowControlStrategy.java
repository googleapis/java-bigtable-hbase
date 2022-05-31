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
package com.google.cloud.bigtable.mirroring.core.utils.flowcontrol;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.MirroringOptions;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface used by {@link FlowController} to decide whether resources needed for performing a
 * secondary database request can be acquired.
 *
 * <p>Implementations of this class should be thread-safe.
 */
@InternalApi("For internal usage only")
public interface FlowControlStrategy {
  /**
   * Returns an object representing a pending request for resources. The returned future will be
   * notified when the request is fulfilled.
   *
   * <p>The user is required to release the resources - otherwise the flow controller will consider
   * them used. More specifically, the user has to either call a successful {@link
   * ListenableFuture#cancel(boolean)} (i.e. returning true) or a successful {@link
   * ListenableFuture#get()} (interrupted get() is not considered successful) followed by calling
   * {@link ResourceReservation#release()} on the returned object.
   */
  ListenableFuture<ResourceReservation> asyncRequestResourceReservation(
      RequestResourcesDescription resourcesDescription);

  /** Releases resources associated with provided description. */
  void releaseResource(RequestResourcesDescription resource);

  interface Factory {
    FlowControlStrategy create(MirroringOptions options) throws Throwable;
  }
}
