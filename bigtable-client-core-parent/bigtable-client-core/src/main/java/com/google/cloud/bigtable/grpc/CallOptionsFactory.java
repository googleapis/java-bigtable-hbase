/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;

/**
 * A factory that creates {@link CallOption}s for use in {@link BigtableDataClient} RPCs.
 */
public interface CallOptionsFactory {

  /**
   * Provide a {@link CallOptions} object to be used in a single RPC. {@link CallOptions} can
   * contain state, specifically start time with an expiration is set; in cases when timeouts are
   * used, implementations should create a new CallOptions each time this method is called.
   *
   * @param descriptor The RPC that's being called. Different methods have different performance
   *          characteristics, so this parameter can be useful to craft the right timeout for the
   *          right method.
   * @param request Some methods, specifically ReadRows, can have variability depending on the
   *          request. The request can be for either a single row, or a range. This parameter can be
   *          used to tune timeouts
   */
  <RequestT> CallOptions create(MethodDescriptor<RequestT, ?> descriptor, RequestT request);

  /**
   * Always returns {@link CallOptions#DEFAULT}.
   */
  public static class Default implements CallOptionsFactory {
    @Override
    public <RequestT> CallOptions create(MethodDescriptor<RequestT, ?> descriptor,
        RequestT request) {
      return CallOptions.DEFAULT;
    }
  }
}
