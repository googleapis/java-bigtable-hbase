/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import io.grpc.Call;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingCall.SimpleForwardingCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * An {@link ClientInterceptor} that updates "User-Agent" header.
 */
public class UserAgentUpdaterInterceptor implements ClientInterceptor {

  private final String userAgent;

  UserAgentUpdaterInterceptor(String userAgent) {
    this.userAgent = userAgent;
  }

  @Override
  public <ReqT, RespT> Call<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      Channel next) {
    return new SimpleForwardingCall<ReqT, RespT>(next.newCall(method)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata.Headers headers) {
        Metadata.Key<String> key = Metadata.Key.of("User-Agent", Metadata.ASCII_STRING_MARSHALLER);
        String userAgents = headers.get(key);
        if (userAgents == null) {
          userAgents = userAgent;
        } else {
          userAgents += " " + userAgent;
        }
        headers.put(key, userAgents);
        super.start(responseListener, headers);
      }
    };
  }
}
