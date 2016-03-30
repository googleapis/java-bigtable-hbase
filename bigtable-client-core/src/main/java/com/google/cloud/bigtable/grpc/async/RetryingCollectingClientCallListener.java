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
import com.google.common.util.concurrent.SettableFuture;

public class RetryingCollectingClientCallListener<RequestT, ResponseT>
    extends AbstractRetryingRpcListener<RequestT, ResponseT, List<ResponseT>> {

  private ImmutableList.Builder<ResponseT> buffer;

  public RetryingCollectingClientCallListener(
      RetryOptions retryOptions,
      RequestT request,
      BigtableAsyncRpc<RequestT, ResponseT> retryableRpc,
      ScheduledExecutorService executorService) {
    super(retryOptions, request, retryableRpc, executorService);
  }

  public RetryingCollectingClientCallListener(
      RetryOptions retryOptions,
      RequestT request,
      BigtableAsyncRpc<RequestT, ResponseT> retryableRpc,
      ScheduledExecutorService executorService,
      SettableFuture<List<ResponseT>> completionFuture) {
    super(retryOptions, request, retryableRpc, executorService, completionFuture);
  }

  @Override
  public void run() {
    buffer = new ImmutableList.Builder<>();
    super.run();
  }

  @Override
  public void onMessage(ResponseT message) {
    call.request(1);
    buffer.add(message);
  }

  @Override
  protected void onOK() {
    getCompletionFuture().set(buffer.build());
  }
}
