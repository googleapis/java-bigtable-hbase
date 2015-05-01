/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;

import io.grpc.Call;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.Objects;

/**
 * A {@link Channel} that records the {@link Status} passed to onClose of each call issued on
 * the channel.
 */
public class CallCompletionStatusInterceptor implements ClientInterceptor {

  /**
   * The final status of a single Call.
   */
  static class CallCompletionStatus {
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

  private final Multiset<CallCompletionStatus> callCompletionStatuses =
      ConcurrentHashMultiset.create();

  /**
   * A {@link Call} that listens for onClose events and records the final {@link Status} for
   * the call.
   * @param <RequestT> The request message type
   * @param <ResponseT> The response message type
   */
  @VisibleForTesting
  class CompletionStatusGatheringCall<RequestT, ResponseT>
      extends ClientInterceptors.ForwardingCall<RequestT, ResponseT> {

    private final MethodDescriptor<RequestT, ResponseT> method;

    public CompletionStatusGatheringCall(
        MethodDescriptor<RequestT, ResponseT> method, Call<RequestT, ResponseT> delegateCall) {
      super(delegateCall);
      this.method = method;
    }

    /**
     * Wrap a Listener that will record the final Call status in onClose.
     */
    Listener<ResponseT> createGatheringListener(Listener<ResponseT> responseListener) {
      return new ClientInterceptors.ForwardingListener<ResponseT>(responseListener) {
        @Override
        public void onClose(Status status, Metadata.Trailers trailers) {
          callCompletionStatuses.add(new CallCompletionStatus(method, status));
          super.onClose(status, trailers);
        }};
    }

    @Override
    public void start(Listener<ResponseT> responseListener, Metadata.Headers headers) {
      Listener<ResponseT> forwardingListener = createGatheringListener(responseListener);
      super.start(forwardingListener, headers);
    }
  }

  @Override
  public <ReqT, RespT> CompletionStatusGatheringCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, Channel channel) {
    return wrapCall(methodDescriptor, channel.newCall(methodDescriptor));
  }

  /**
   * Wrap an existing call in a new CompletionStatusGatheringCall.
   */
  private <ReqT, RespT> CompletionStatusGatheringCall<ReqT, RespT> wrapCall(
      MethodDescriptor<ReqT, RespT> method, Call<ReqT, RespT> call) {
    return new CompletionStatusGatheringCall<>(method, call);
  }

  /**
   * Retrieve {@link CallCompletionStatus} instances that have been accumulated on this Channel.
   */
  public Multiset<CallCompletionStatus> getCallCompletionStatuses() {
    return callCompletionStatuses;
  }
}
