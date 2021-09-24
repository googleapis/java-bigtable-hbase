/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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

import com.google.auth.Credentials;
import io.grpc.*;
import io.grpc.auth.MoreCallCredentials;

public class CredentialsInterceptor implements ClientInterceptor {

  private CallCredentials callCredentials;

  public CredentialsInterceptor(Credentials credentials) {
    this.callCredentials = MoreCallCredentials.from(credentials);
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        channel.newCall(methodDescriptor, callOptions.withCallCredentials(callCredentials))) {};
  }
}
