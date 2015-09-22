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
package com.google.cloud.bigtable.grpc.io;

import io.grpc.ClientCall;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A {@link Channel} that records the {@link Status} passed to onClose of each call issued on
 * the channel.
 */
public class CallCompletionStatusInterceptor implements ClientInterceptor {

  /**
   * The final status of a single ClientCall.
   */
  public static class CallCompletionStatus {
    private final MethodDescriptor<?, ?> method;
    private final Status callStatus;

    public CallCompletionStatus(MethodDescriptor<?, ?> method, Status callStatus) {
      this.method = method;
      this.callStatus = callStatus;
    }

    /**
     * Get the method that was invoked triggering this status.
     */
    public MethodDescriptor<?, ?> getMethod() {
      return method;
    }

    /**
     * Get the gRPC status for this call.
     */
    public Status getCallStatus() {
      return callStatus;
    }

    @Override
    public int hashCode() {
      return Objects.hash(method, callStatus.getCode());
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof CallCompletionStatus)) {
        return false;
      }
      CallCompletionStatus other = (CallCompletionStatus) obj;
      return Objects.equals(method, other.method)
          && Objects.equals(callStatus, other.callStatus);
    }
  }

  // Executor on which to update statuses.
  private final ExecutorService countUpdateExecutor;
  // This will only be updated by the countUpdateExecutor, but can still be read
  // elsewhere
  private final Multiset<CallCompletionStatus> callCompletionStatuses =
      ConcurrentHashMultiset.create();

  /**
   * Construct an interceptor that uses a single thread for updating statuses.
   */
  public CallCompletionStatusInterceptor() {
    this(Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat("call-status-recorder")
            .setDaemon(true)
            .build()));
  }

  /**
   * Construct an interceptor that uses the given ExecutorService.
   */
  public CallCompletionStatusInterceptor(ExecutorService executorService) {
    this.countUpdateExecutor = executorService;
  }

  /**
   * A {@link ClientCall} that listens for onClose events and records the final {@link Status} for
   * the call.
   * @param <RequestT> The request message type
   * @param <ResponseT> The response message type
   */
  @VisibleForTesting
  class CompletionStatusGatheringCall<RequestT, ResponseT>
      extends SimpleForwardingClientCall<RequestT, ResponseT> {

    private final MethodDescriptor<RequestT, ResponseT> method;

    public CompletionStatusGatheringCall(
        MethodDescriptor<RequestT, ResponseT> method, ClientCall<RequestT, ResponseT> delegateCall) {
      super(delegateCall);
      this.method = method;
    }

    /**
     * Wrap a Listener that will record the final ClientCall status in onClose.
     */
    ClientCall.Listener<ResponseT> createGatheringListener(Listener<ResponseT> responseListener) {
      return new ForwardingClientCallListener.SimpleForwardingClientCallListener<ResponseT>(
          responseListener) {
        @Override
        public void onClose(final Status status, Metadata trailers) {
          countUpdateExecutor.submit(new Runnable() {
            @Override
            public void run() {
              callCompletionStatuses.add(new CallCompletionStatus(method, status));
            }
          });
          super.onClose(status, trailers);
        }
      };
    }

    @Override
    public void start(Listener<ResponseT> responseListener, Metadata headers) {
      Listener<ResponseT> forwardingListener = createGatheringListener(responseListener);
      super.start(forwardingListener, headers);
    }
  }

  @Override
  public <RequestT, ResponseT> CompletionStatusGatheringCall<RequestT, ResponseT> interceptCall(
      MethodDescriptor<RequestT, ResponseT> method, CallOptions callOptions, Channel next) {
    return wrapCall(method, next.newCall(method, callOptions));
  }

  /**
   * Wrap an existing call in a new CompletionStatusGatheringCall.
   */
  private <ReqT, RespT> CompletionStatusGatheringCall<ReqT, RespT> wrapCall(
      MethodDescriptor<ReqT, RespT> method, ClientCall<ReqT, RespT> call) {
    return new CompletionStatusGatheringCall<>(method, call);
  }

  /**
   * Retrieve {@link CallCompletionStatus} instances that have been accumulated on this Channel.
   */
  public Multiset<CallCompletionStatus> getCallCompletionStatuses() {
    return callCompletionStatuses;
  }
}
