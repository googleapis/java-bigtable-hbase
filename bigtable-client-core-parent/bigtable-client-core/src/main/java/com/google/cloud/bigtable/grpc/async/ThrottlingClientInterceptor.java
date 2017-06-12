/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.protobuf.MessageLite;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/**
 * Throttles requests based on {@link ResourceLimiter}
 *
 */
public class ThrottlingClientInterceptor implements ClientInterceptor {

  private ResourceLimiter resourceLimiter;

  public void enable(ResourceLimiter resourceLimiter) {
    Preconditions.checkNotNull(resourceLimiter);
    Preconditions
        .checkArgument(this.resourceLimiter == null || this.resourceLimiter == resourceLimiter);
    this.resourceLimiter = resourceLimiter;
  }

  @Override
  public <ReqT , RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    if (resourceLimiter == null) {
      return next.newCall(method, callOptions);
    }

    final AtomicLong id = new AtomicLong(-1);
    final AtomicBoolean interrupted = new AtomicBoolean(false);
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> delegate, Metadata headers) {
        if (interrupted.get()) {
          if (id.get() != -1) {
            resourceLimiter.markCanBeCompleted(id.get());
          }
          delegate.onClose(Status.INTERNAL.withDescription("Operation was interrupted"),
            new Metadata());
        } else {
          SimpleForwardingClientCallListener<RespT> listener =
              new SimpleForwardingClientCallListener<RespT>(delegate) {
                public void onClose(io.grpc.Status status, Metadata trailers) {
                  resourceLimiter.markCanBeCompleted(id.get());
                }
              };
          delegate().start(listener, headers);
        }
      }

      @Override
      public void sendMessage(ReqT message) {
        try {
          id.set(resourceLimiter.registerOperationWithHeapSize(((MessageLite)message).getSerializedSize()));
          super.sendMessage(message);
        } catch (InterruptedException e) {
          interrupted.set(true);
        }
      }
    };
  }

}
