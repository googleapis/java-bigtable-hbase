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

import com.google.api.core.InternalApi;
import com.google.common.base.Preconditions;
import com.google.protobuf.MessageLite;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.concurrent.CancellationException;
import javax.annotation.Nullable;

/**
 * Throttles requests based on {@link ResourceLimiter}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ThrottlingClientInterceptor implements ClientInterceptor {
  private final ResourceLimiter resourceLimiter;

  public ThrottlingClientInterceptor(ResourceLimiter resourceLimiter) {
    Preconditions.checkNotNull(resourceLimiter);
    this.resourceLimiter = resourceLimiter;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      final MethodDescriptor<ReqT, RespT> method,
      final CallOptions callOptions,
      final Channel delegateChannel) {
    if (resourceLimiter == null) {
      return delegateChannel.newCall(method, callOptions);
    }

    return new ClientCall<ReqT, RespT>() {
      private boolean cancelledEarly;

      private ClientCall.Listener<RespT> delegateListener = null;
      private ClientCall<ReqT, RespT> delegateCall;
      private Metadata headers;
      private int numMessagesRequested = 0;
      private Long id = null;

      @Override
      public void start(ClientCall.Listener<RespT> listener, Metadata headers) {
        Preconditions.checkState(!cancelledEarly, "Call already cancelled");
        Preconditions.checkState(
            this.delegateListener == null && this.headers == null, "Call already started");

        this.delegateListener = Preconditions.checkNotNull(listener);
        this.headers = Preconditions.checkNotNull(headers);
      }

      @Override
      public void request(int numMessages) {
        if (delegateCall != null) {
          delegateCall.request(numMessages);
          return;
        }

        Preconditions.checkState(!cancelledEarly, "Call already cancelled");

        // request() might be called multiple times before sendMessage(), so add up all of the
        // requested sendMessages.
        numMessagesRequested += numMessages;
      }

      @Override
      public void cancel(@Nullable String message, @Nullable Throwable cause) {
        if (delegateCall != null) {
          delegateCall.cancel(message, cause);
          return;
        }

        cancelledEarly = true;

        if (message == null && cause == null) {
          cause = new CancellationException("Cancelled without a message or cause");
        }

        if (delegateListener != null) {
          delegateListener.onClose(
              Status.CANCELLED.withDescription(message).withCause(cause), new Metadata());
        }
      }

      @Override
      public void sendMessage(ReqT message) {
        Preconditions.checkState(
            delegateCall == null, "ThrottlingClientInterceptor only supports unary operations");
        Preconditions.checkState(
            delegateListener != null && headers != null,
            "start() has to be called before sendMessage().");
        Preconditions.checkState(!cancelledEarly, "Call already cancelled");

        try {
          id =
              resourceLimiter.registerOperationWithHeapSize(
                  ((MessageLite) message).getSerializedSize());
        } catch (InterruptedException e) {
          delegateListener.onClose(
              Status.INTERNAL.withDescription("Operation was interrupted"), new Metadata());
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
        delegateCall.start(markCompletionListener, headers);
        delegateCall.request(numMessagesRequested);
        delegateCall.sendMessage(message);
        delegateCall.halfClose();
      }

      @Override
      public void halfClose() {
        // Noop
      }
    };
  }
}
