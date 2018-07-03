/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.io;

import com.google.api.core.InternalApi;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import java.util.Set;

/**
 * Internal implementation detail to prevent RPC from hanging.
 */
@InternalApi
public class WatchdogInterceptor implements ClientInterceptor {
  private final Set<MethodDescriptor<?,?>> watchedMethods;
  private final Watchdog watchdog;


  public WatchdogInterceptor(Set<MethodDescriptor<?, ?>> watchedMethods, Watchdog watchdog) {
    this.watchedMethods = watchedMethods;
    this.watchdog = watchdog;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

    ClientCall<ReqT, RespT> call = channel.newCall(methodDescriptor, callOptions);
    if (watchedMethods.contains(methodDescriptor)) {
      call = watchdog.watch(call);
    }

    return call;
  }
}
