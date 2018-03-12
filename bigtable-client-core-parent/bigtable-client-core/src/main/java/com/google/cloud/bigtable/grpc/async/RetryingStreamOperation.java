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

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.google.cloud.bigtable.config.RetryOptions;
import com.google.common.collect.ImmutableList;

import io.grpc.CallOptions;
import io.grpc.Metadata;

/**
 * An extension of {@link AbstractRetryingOperation} that aggregates all responses from a streaming
 * request into a List.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class RetryingStreamOperation<RequestT, ResponseT>
    extends AbstractRetryingOperation<RequestT, ResponseT, List<ResponseT>> {

  private ImmutableList.Builder<ResponseT> buffer;

  /**
   * <p>Constructor for RetryingCollectingClientCallListener.</p>
   *
   * @param retryOptions a {@link com.google.cloud.bigtable.config.RetryOptions} object.
   * @param request a RequestT object.
   * @param retryableRpc a {@link com.google.cloud.bigtable.grpc.async.BigtableAsyncRpc} object.
   * @param callOptions a {@link io.grpc.CallOptions} object.
   * @param executorService a {@link java.util.concurrent.ScheduledExecutorService} object.
   * @param metadata a {@link io.grpc.Metadata} object.
   */
  public RetryingStreamOperation(RetryOptions retryOptions, RequestT request,
      BigtableAsyncRpc<RequestT, ResponseT> retryableRpc, CallOptions callOptions,
      ScheduledExecutorService executorService, Metadata metadata) {
    super(retryOptions, request, retryableRpc, callOptions, executorService, metadata);
  }

  /** {@inheritDoc} */
  @Override
  public void run() {
    buffer = new ImmutableList.Builder<>();
    super.run();
  }

  /** {@inheritDoc} */
  @Override
  public void onMessage(ResponseT message) {
    getCall().request(1);
    buffer.add(message);
  }

  /** {@inheritDoc} */
  @Override
  protected boolean onOK(Metadata trailers) {
    completionFuture.set(buffer.build());
    return true;
  }
}
