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
package com.google.cloud.bigtable.grpc.async;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * This interface represents a logical asynchronous RPC end point, including creating a
 * {@link ClientCall} for a new request.
 */
public interface BigtableAsyncRpc<REQUEST, RESPONSE> {
  /**
   * Creates a {@link ClientCall}.
   *
   * @param request The request to send.
   * @param listener A listener which handles responses.
   * @param callOptions A set of gRPC options to use on this call.
   * @param metadata A set of predefined headers to use.
   * @return A ClientCall that represents a new request.
   */
  ClientCall<REQUEST, RESPONSE> call(REQUEST request, ClientCall.Listener<RESPONSE> listener,
      CallOptions callOptions, Metadata metadata);

  /**
   * Can this request be retried?
   *
   * @param request The request to send which may require introspection to determine retryability.
   * @return true if the request can be retried.
   */
  boolean isRetryable(REQUEST request);

  /**
   * @return {@link MethodDescriptor} that describes the logical endpoint.
   */
  MethodDescriptor<REQUEST, RESPONSE> getMethodDescriptor();
}
