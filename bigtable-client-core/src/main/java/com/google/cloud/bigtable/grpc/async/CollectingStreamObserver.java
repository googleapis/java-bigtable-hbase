package com.google.cloud.bigtable.grpc.async;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * CollectingStreamObserver buffers all stream messages in an internal
 * List and signals the result of {@link #getResponseCompleteFuture()} when complete.
 */
public class CollectingStreamObserver<T> implements StreamObserver<T> {
  private final SettableFuture<List<T>> responseCompleteFuture = SettableFuture.create();
  private final List<T> buffer = new ArrayList<>();

  public ListenableFuture<List<T>> getResponseCompleteFuture() {
    return responseCompleteFuture;
  }

  @Override
  public void onValue(T value) {
    buffer.add(value);
  }

  @Override
  public void onError(Throwable throwable) {
    responseCompleteFuture.setException(throwable);
  }

  @Override
  public void onCompleted() {
    responseCompleteFuture.set(buffer);
  }
}