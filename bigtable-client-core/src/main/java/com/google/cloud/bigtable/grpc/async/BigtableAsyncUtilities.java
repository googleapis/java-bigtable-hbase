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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.io.CancellationToken;
import com.google.cloud.bigtable.grpc.io.ClientCallService;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;

/**
 * Utilities for creating and executing async methods.
 */
public final class BigtableAsyncUtilities {

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

  public static BigtableAsyncRpc<SampleRowKeysRequest, List<SampleRowKeysResponse>>
      createSampleRowKeyAsyncReader(Channel channel, ClientCallService clientCallService) {
    return createStreamingAsyncRpc(channel, BigtableServiceGrpc.METHOD_SAMPLE_ROW_KEYS,
      IMMUTABLE_LIST_TRANSFORMER, clientCallService);
  }

  public static BigtableAsyncRpc<ReadRowsRequest, List<Row>> createRowKeyAysncReader(Channel channel,
      ClientCallService clientCallService) {
    return createStreamingAsyncRpc(channel, BigtableServiceGrpc.METHOD_READ_ROWS, ROW_TRANSFORMER,
      clientCallService);
  }

  public static <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT> createAsyncUnaryRpc(
      final Channel channel,
      final ClientCallService clientCallService,
      final MethodDescriptor<RequestT, ResponseT> method) {
    return new BigtableAsyncRpc<RequestT, ResponseT>() {
      @Override
      public ListenableFuture<ResponseT> call(RequestT request) {
        final ClientCall<RequestT, ResponseT> call = channel.newCall(method, CallOptions.DEFAULT);
        return clientCallService.listenableAsyncCall(call, request);
      }
    };
  }

  public static <RequestT, ResponseT> BigtableAsyncRpc<RequestT, ResponseT> createAsyncUnaryRpc(
      final Channel channel,
      final ClientCallService clientCallService,
      final MethodDescriptor<RequestT, ResponseT> method,
      final CancellationToken token) {
    return new BigtableAsyncRpc<RequestT, ResponseT>() {
      @Override
      public ListenableFuture<ResponseT> call(RequestT request) {
        final ClientCall<RequestT, ResponseT> call = channel.newCall(method, CallOptions.DEFAULT);
        token.addListener(new Runnable(){
          @Override
          public void run() {
            call.cancel();
          }
        }, MoreExecutors.directExecutor());
        return clientCallService.listenableAsyncCall(call, request);
      }
    };
  }

  private static <RequestT, ResponseT, OutputT>
      BigtableAsyncRpc<RequestT, List<OutputT>> createStreamingAsyncRpc(
          final Channel channel,
          final MethodDescriptor<RequestT, ResponseT> method,
          final Function<List<ResponseT>, List<OutputT>> function,
          final ClientCallService clientCallService) {
    return new BigtableAsyncRpc<RequestT, List<OutputT>>() {
      @Override
      public ListenableFuture<List<OutputT>> call(RequestT request) {
        ClientCall<RequestT, ResponseT> call = channel.newCall(method, CallOptions.DEFAULT);
        CollectingStreamObserver<ResponseT> responseCollector = new CollectingStreamObserver<>();
        clientCallService.asyncServerStreamingCall(call, request, responseCollector);
        return Futures.transform(responseCollector.getResponseCompleteFuture(), function);
      }
    };
  }

  /**
   * Performs the rpc with retries.
   *
   * @param retryOptions Configures how to perform backoffs when failures occur.
   * @param request The request to send.
   * @param rpc The rpc to perform
   * @param executorService The ExecutorService on which to run if there is a need for a retry.
   * @return the ListenableFuture that can be used to track the RPC.
   */
  public static <RequestT, ResponseT> ListenableFuture<ResponseT> performRetryingAsyncRpc(
      RetryOptions retryOptions,
      final RequestT request,
      BigtableAsyncRpc<RequestT, ResponseT> rpc,
      ExecutorService executorService) {
    ListenableFuture<ResponseT> listenableFuture = rpc.call(request);
    if (retryOptions.enableRetries()) {
      return Futures.catchingAsync(
          listenableFuture,
          StatusRuntimeException.class,
          RetryingRpcFunction.create(retryOptions, request, rpc),
          executorService);
    } else {
      return listenableFuture;
    }
  }

  /**
   * Returns the result of calling {@link Future#get()} interruptably on a task known not to throw a
   * checked exception.
   *
   * <p>If interrupted, the interrupt is restored before throwing an exception..
   *
   * @throws java.util.concurrent.CancellationException
   *     if {@code get} throws a {@code CancellationException}.
   * @throws io.grpc.StatusRuntimeException if {@code get} throws an {@link ExecutionException}
   *     or an {@link InterruptedException}.
   *     
   * @see ClientCalls#getUnchecked(Future)
   */
  public static <V> V getUnchecked(Future<V> future) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Status.CANCELLED.withCause(e).asRuntimeException();
    } catch (ExecutionException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }
  private BigtableAsyncUtilities(){
  }
}
