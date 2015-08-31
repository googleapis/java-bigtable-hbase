package com.google.cloud.bigtable.grpc.async;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public final class ReadAsyncFactory {

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

  public static ReadAsync<SampleRowKeysRequest, SampleRowKeysResponse>
      createSampleRowKeyAsyncReader(final Channel channel) {
    return createReadAsync(channel, BigtableServiceGrpc.METHOD_SAMPLE_ROW_KEYS,
      IMMUTABLE_LIST_TRANSFORMER);
  }

  public static ReadAsync<ReadRowsRequest, Row> createRowKeyAysncReader(final Channel channel) {
    return createReadAsync(channel, BigtableServiceGrpc.METHOD_READ_ROWS, ROW_TRANSFORMER);
  }

  private static <RequestT, ResponseT, OutputT> ReadAsync<RequestT, OutputT> createReadAsync(
      final Channel channel, final MethodDescriptor<RequestT, ResponseT> method,
      final Function<List<ResponseT>, List<OutputT>> function) {
    return new ReadAsync<RequestT, OutputT>() {
      @Override
      public ListenableFuture<List<OutputT>> readAsync(RequestT request) {
        ClientCall<RequestT, ResponseT> readRowsCall =
            channel.newCall(method, CallOptions.DEFAULT);
        CollectingStreamObserver<ResponseT> responseCollector = new CollectingStreamObserver<>();
        ClientCalls.asyncServerStreamingCall(readRowsCall, request, responseCollector);
        return Futures.transform(responseCollector.getResponseCompleteFuture(), function);
      }
    };
  }

  private ReadAsyncFactory(){
  }
}
