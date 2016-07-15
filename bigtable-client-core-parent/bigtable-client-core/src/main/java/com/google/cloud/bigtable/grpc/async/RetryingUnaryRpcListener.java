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

import java.util.concurrent.ScheduledExecutorService;

import com.google.cloud.bigtable.config.RetryOptions;
import com.google.common.util.concurrent.AsyncFunction;

import io.grpc.CallOptions;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * A {@link AsyncFunction} that retries a {@link BigtableAsyncRpc} request.
 */
public class RetryingUnaryRpcListener<RequestT, ResponseT>
    extends AbstractRetryingRpcListener<RequestT, ResponseT, ResponseT> {
  final static StatusRuntimeException NO_VALUE_SET_EXCEPTION =
      Status.INTERNAL.withDescription("No value received for unary call").asRuntimeException();

  private ResponseT value;

  public RetryingUnaryRpcListener(RetryOptions retryOptions, RequestT request,
      BigtableAsyncRpc<RequestT, ResponseT> retryableRpc, CallOptions callOptions,
      ScheduledExecutorService executorService, Metadata metadata) {
    super(retryOptions, request, retryableRpc, callOptions, executorService, metadata);
  }

  @Override
  public void onMessage(ResponseT message) {
    value = message;
    completionFuture.set(value);
  }

  @Override
  protected void onOK() {
    if (value == null) {
      // No value received so mark the future as an error
      completionFuture.setException(NO_VALUE_SET_EXCEPTION);
    }
  }
}