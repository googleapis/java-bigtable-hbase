package com.google.cloud.bigtable.grpc.async;

import io.grpc.stub.StreamObserver;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * A StreamObserver for unary async operations. It assumes that the operation is complete
 * as soon as a single response is received.
 * @param <T> The response type.
 */
public class AsyncUnaryOperationObserver<T> implements StreamObserver<T> {
  private final SettableFuture<T> completionFuture = SettableFuture.create();

  @Override
  public void onValue(T t) {
    completionFuture.set(t);
  }

  @Override
  public void onError(Throwable throwable) {
    completionFuture.setException(throwable);
  }

  @Override
  public void onCompleted() {
  }

  public ListenableFuture<T> getCompletionFuture() {
    return completionFuture;
  }
}