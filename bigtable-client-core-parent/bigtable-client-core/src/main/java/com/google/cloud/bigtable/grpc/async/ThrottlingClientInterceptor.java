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

import com.google.common.base.Preconditions;
import com.google.protobuf.MessageLite;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/**
 * Throttles requests based on {@link ResourceLimiter}
 *
 */
public class ThrottlingClientInterceptor implements ClientInterceptor {

  private final ResourceLimiter resourceLimiter;

  public ThrottlingClientInterceptor(ResourceLimiter resourceLimiter) {
    Preconditions.checkNotNull(resourceLimiter);
    this.resourceLimiter = resourceLimiter;
  }

  @Override
  public <ReqT , RespT> ClientCall<ReqT, RespT> interceptCall(
      final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions, final Channel delegateChannel) {
    if (resourceLimiter == null) {
      return delegateChannel.newCall(method, callOptions);
    }

    return new ForwardingClientCall<ReqT, RespT>() {
      private ClientCall.Listener<RespT> delegateListener = null;
      private ClientCall<ReqT, RespT> delegateCall;
      private Metadata headers;
      private int numMessagesRequested = 0;
      private Long id = null;

      @Override
      public void start(ClientCall.Listener<RespT> listener, Metadata headers) {
        this.delegateListener = listener;
        this.headers = headers;
      }

      @Override
      public void request(int numMessages) {
        if (id == null) {
          // request() might be called multiple times before sendMessage(), so add up all of the
          // requested sendMessages.
          numMessagesRequested += numMessages;
        } else {
          delegate().request(numMessages);
        }
      }

      @Override
      public void sendMessage(ReqT message) {
        Preconditions.checkState(delegateCall == null,
          "ThrottlingClientInterceptor only supports unary operations");
        Preconditions.checkState(id == null,
          "ThrottlingClientInterceptor only supports unary operations");
        Preconditions.checkState(delegateListener != null,
          "start() has to be called before sendMessage().");
        Preconditions.checkState(headers != null, "start() has to be called before sendMessage().");
        try {
          id = resourceLimiter
              .registerOperationWithHeapSize(((MessageLite) message).getSerializedSize());
        } catch (InterruptedException e) {
          delegateListener.onClose(Status.INTERNAL.withDescription("Operation was interrupted"),
            new Metadata());
          return;
        }
        delegateCall = delegateChannel.newCall(method, callOptions);
        SimpleForwardingClientCallListener<RespT> markCompletionListener =
            new SimpleForwardingClientCallListener<RespT>(this.delegateListener) {
              @Override
              public void onClose(io.grpc.Status status, Metadata trailers) {
                resourceLimiter.markCanBeCompleted(id);
                delegate().onClose(status, trailers);
              }
            };
        delegate().start(markCompletionListener, headers);
        delegate().request(numMessagesRequested);
        delegate().sendMessage(message);
      }

      @Override
      protected ClientCall<ReqT, RespT> delegate() {
        return delegateCall;
      }
    };
  }
}
