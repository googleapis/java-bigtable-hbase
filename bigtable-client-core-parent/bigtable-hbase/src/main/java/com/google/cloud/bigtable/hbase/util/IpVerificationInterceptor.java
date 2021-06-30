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
package com.google.cloud.bigtable.hbase.util;

import com.google.api.core.InternalApi;
import com.google.common.base.Preconditions;
import io.grpc.CallOptions;
import io.grpc.CallOptions.Key;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.net.SocketAddress;
import java.util.regex.Pattern;

@InternalApi("for testing only")
public class IpVerificationInterceptor implements ClientInterceptor {
  public static CallOptions.Key<Boolean> SKIP_IP_VERIFICATION =
      Key.createWithDefault("SKIP_IP_VERIFICATION", false);
  private final Pattern regex;

  public IpVerificationInterceptor(Pattern regex) {
    this.regex = regex;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    final ClientCall<ReqT, RespT> clientCall = next.newCall(method, callOptions);

    if (callOptions.getOption(SKIP_IP_VERIFICATION)) {
      return clientCall;
    }

    return new SimpleForwardingClientCall<ReqT, RespT>(clientCall) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        super.start(
            new SimpleForwardingClientCallListener<RespT>(responseListener) {
              @Override
              public void onHeaders(Metadata headers) {
                // Check peer IP after connection is established.
                SocketAddress remoteAddr =
                    clientCall.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);

                Preconditions.checkNotNull(
                    remoteAddr, "Call was missing TRANSPORT_ATTR_REMOTE_ADDR");

                Preconditions.checkState(
                    regex.matcher(remoteAddr.toString()).matches(),
                    "Remote address %s didn't match the pattern %s",
                    remoteAddr,
                    regex);

                super.onHeaders(headers);
              }
            },
            headers);
      }
    };
  }
}
