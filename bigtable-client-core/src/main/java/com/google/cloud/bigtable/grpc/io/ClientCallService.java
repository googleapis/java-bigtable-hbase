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
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;

import com.google.cloud.bigtable.grpc.async.AsyncUnaryOperationObserver;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Wraps gRPC's {@link ClientCall} methods to allow for unit testing.
 */
public interface ClientCallService {

  <ReqT, RespT> void asyncServerStreamingCall(ClientCall<ReqT, RespT> call,
      ReqT request, StreamObserver<RespT> observer);


  <ReqT, RespT> Iterator<RespT> blockingServerStreamingCall(ClientCall<ReqT, RespT> call,
      ReqT request);

  <ReqT, RespT> RespT blockingUnaryCall(ClientCall<ReqT, RespT> call, ReqT request);

  <ReqT, RespT> ListenableFuture<RespT> listenableAsyncCall(ClientCall<ReqT, RespT> call,
      ReqT request);

  ClientCallService DEFAULT = new ClientCallService() {

    @Override
    public <ReqT, RespT> void asyncServerStreamingCall(ClientCall<ReqT, RespT> call,
        ReqT request, StreamObserver<RespT> observer) {
      ClientCalls.asyncServerStreamingCall(call, request, observer);
    }

    @Override
    public <ReqT, RespT> Iterator<RespT> blockingServerStreamingCall(ClientCall<ReqT, RespT> call,
        ReqT request) {
      return ClientCalls.blockingServerStreamingCall(call, request);
    }

    @Override
    public <ReqT, RespT> RespT blockingUnaryCall(ClientCall<ReqT, RespT> call, ReqT request) {
      return ClientCalls.blockingUnaryCall(call, request);
    }

    @Override
    public <ReqT, RespT> ListenableFuture<RespT> listenableAsyncCall(ClientCall<ReqT, RespT> call,
        ReqT request) {
      AsyncUnaryOperationObserver<RespT> observer = new AsyncUnaryOperationObserver<>();
      ClientCalls.asyncUnaryCall(call, request, observer);
      return observer.getCompletionFuture();
    }
  };

}
