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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

/**
 * Utilities for creating and executing async methods.
 */
public interface BigtableAsyncUtilities {

  BigtableAsyncRpc<SampleRowKeysRequest, List<SampleRowKeysResponse>>
      createSampleRowKeyAsyncReader();

  BigtableAsyncRpc<ReadRowsRequest, List<Row>> createRowKeyAysncReader();

  <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT>
      createAsyncUnaryRpc(MethodDescriptor<RequestT, ResponseT> method);

  /**
   * Performs the rpc with retries.
   *
   * @param request The request to send.
   * @param rpc The rpc to perform
   * @param executorService The ExecutorService on which to run if there is a need for a retry.
   * @return the ListenableFuture that can be used to track the RPC.
   */
  <RequestT, ResponseT> ListenableFuture<ResponseT> performRetryingAsyncRpc(RequestT request,
      BigtableAsyncRpc<RequestT, ResponseT> rpc, CancellationToken cancellationToken,
      ExecutorService executorService);

  <RequestT, ResponseT> void asyncServerStreamingCall(ClientCall<RequestT, ResponseT> call,
      RequestT request, ClientCall.Listener<ResponseT> listener);

  public static class Default implements BigtableAsyncUtilities {
    private static final Function<List<SampleRowKeysResponse>, List<SampleRowKeysResponse>> IMMUTABLE_LIST_TRANSFORMER =
        new Function<List<SampleRowKeysResponse>, List<SampleRowKeysResponse>>() {
          @Override
          public List<SampleRowKeysResponse> apply(List<SampleRowKeysResponse> list) {
            return ImmutableList.copyOf(list);
          }
        };

    private static Function<List<ReadRowsResponse>, List<Row>> ROW_TRANSFORMER =
        new Function<List<ReadRowsResponse>, List<Row>>() {
          @Override
          public List<Row> apply(List<ReadRowsResponse> responses) {
            List<Row> result = new ArrayList<>();
            Iterator<ReadRowsResponse> responseIterator = responses.iterator();
            while (responseIterator.hasNext()) {
              result.add(RowMerger.readNextRow(responseIterator));
            }
            return result;
          }
        };

    private final Channel channel;
    private final RetryOptions retryOptions;

    public Default(Channel channel, RetryOptions retryOptions) {
      this.channel = channel;
      this.retryOptions = retryOptions;
    }

    @Override
    public BigtableAsyncRpc<SampleRowKeysRequest, List<SampleRowKeysResponse>>
        createSampleRowKeyAsyncReader() {
      return createStreamingAsyncRpc(BigtableServiceGrpc.METHOD_SAMPLE_ROW_KEYS,
        IMMUTABLE_LIST_TRANSFORMER);
    }

    @Override
    public BigtableAsyncRpc<ReadRowsRequest, List<Row>> createRowKeyAysncReader() {
      return createStreamingAsyncRpc(BigtableServiceGrpc.METHOD_READ_ROWS, ROW_TRANSFORMER);
    }

    @Override
    public <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT>
        createAsyncUnaryRpc(final MethodDescriptor<RequestT, ResponseT> method) {
      return new BigtableAsyncRpc<RequestT, ResponseT>() {
        @Override
        public ListenableFuture<ResponseT> call(RequestT request,
            CancellationToken cancellationToken) {
          AsyncUnaryOperationObserver<ResponseT> listener = new AsyncUnaryOperationObserver<>();
          ClientCall<RequestT, ResponseT> call = channel.newCall(method, CallOptions.DEFAULT);
          // Initially ask for two responses from flow-control so that if a misbehaving server sends
          // more than one responses, we can catch it and fail it in the listener.
          //
          // See ClientCalls.startCall() for more information.
          start(call, request, listener, 2);
          addCancellationListener(cancellationToken, call);
          return listener.getCompletionFuture();
        }
      };
    }

    private <RequestT, ResponseT, OutputT> BigtableAsyncRpc<RequestT, List<OutputT>>
        createStreamingAsyncRpc(final MethodDescriptor<RequestT, ResponseT> method,
            final Function<List<ResponseT>, List<OutputT>> function) {
      return new BigtableAsyncRpc<RequestT, List<OutputT>>() {
        @Override
        public ListenableFuture<List<OutputT>> call(RequestT request,
            CancellationToken cancellationToken) {
          ClientCall<RequestT, ResponseT> call = channel.newCall(method, CallOptions.DEFAULT);
          addCancellationListener(cancellationToken, call);
          CollectingClientCallListener<ResponseT> responseCollector =
              new CollectingClientCallListener<>(call);
          asyncServerStreamingCall(call, request, responseCollector);
          return Futures.transform(responseCollector.getResponseCompleteFuture(), function);
        }
      };
    }

    @Override
    public <RequestT, ResponseT> void asyncServerStreamingCall(ClientCall<RequestT, ResponseT> call,
        RequestT request, ClientCall.Listener<ResponseT> listener) {
      // gRPC treats streaming and unary calls differently for the number of responses to retrieve.
      // See createAsyncUnaryRpc for how unary calls are handled.
      //
      // See ClientCalls.startCall() for more information.
      start(call, request, listener, 1);
    }

    private static <RequestT, ResponseT> void start(ClientCall<RequestT, ResponseT> call,
        RequestT request, ClientCall.Listener<ResponseT> listener, int requestCount) {
      call.start(listener, new Metadata());
      call.request(requestCount);
      try {
        call.sendMessage(request);
        call.halfClose();
      } catch (Throwable t) {
        call.cancel();
        throw Throwables.propagate(t);
      }
    }

    private static void addCancellationListener(CancellationToken token,
        final ClientCall<?, ?> call) {
      if (token != null) {
        token.addListener(new Runnable() {
          @Override
          public void run() {
            call.cancel();
          }
        }, MoreExecutors.directExecutor());
      }
    }

    @Override
    public <RequestT, ResponseT> ListenableFuture<ResponseT> performRetryingAsyncRpc(
        RequestT request,
        BigtableAsyncRpc<RequestT, ResponseT> rpc,
        CancellationToken cancellationToken,
        ExecutorService executorService) {
      if (retryOptions.enableRetries()) {
        RetryingRpcFunction<RequestT, ResponseT> retryingRpcFunction =
            new RetryingRpcFunction<>(retryOptions, request, rpc, executorService, cancellationToken);
        return retryingRpcFunction.callRpcWithRetry();
      } else {
        return rpc.call(request, cancellationToken);
      }
    }
  };

}
