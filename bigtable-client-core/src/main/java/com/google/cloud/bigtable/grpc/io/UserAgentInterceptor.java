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
package com.google.cloud.bigtable.grpc.io;

import com.google.common.net.HttpHeaders;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * An {@link ClientInterceptor} that updates "User-Agent" header.
 */
public class UserAgentInterceptor implements ClientInterceptor {

  private final static Metadata.Key<String> USER_AGENT_KEY =
      Metadata.Key.of(HttpHeaders.USER_AGENT, Metadata.ASCII_STRING_MARSHALLER);

  private final String userAgent;

  public UserAgentInterceptor(String userAgent) {
    this.userAgent = userAgent;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata.Headers headers) {
        String userAgents = headers.get(USER_AGENT_KEY);
        if (userAgents == null) {
          userAgents = userAgent;
        } else {
          userAgents += " " + userAgent;
        }
        headers.put(USER_AGENT_KEY, userAgents);
        super.start(responseListener, headers);
      }
    };
  }
}
