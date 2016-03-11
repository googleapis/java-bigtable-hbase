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

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * {@link CollectingClientCallListener} buffers all stream messages in an internal
 * List and signals the result of {@link #getResponseCompleteFuture()} when complete.
 */
public class CollectingClientCallListener<T> extends ClientCall.Listener<T> {
  private final SettableFuture<List<T>> responseCompleteFuture = SettableFuture.create();
  private final List<T> buffer = new ArrayList<>();
  private final ClientCall<?, T> call;

  public CollectingClientCallListener(ClientCall<?, T> call) {
    this.call = call;
  }

  public ListenableFuture<List<T>> getResponseCompleteFuture() {
    return responseCompleteFuture;
  }

  @Override
  public void onMessage(T message) {
    call.request(1);
    buffer.add(message);
  }

  @Override
  public void onClose(Status status, Metadata trailers) {
    if (status.isOk()) {
      responseCompleteFuture.set(buffer);
    } else {
      responseCompleteFuture.setException(status.asRuntimeException());
    }
  }
}