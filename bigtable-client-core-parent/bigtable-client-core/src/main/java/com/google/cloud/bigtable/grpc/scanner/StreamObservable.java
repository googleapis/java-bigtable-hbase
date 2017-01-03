package com.google.cloud.bigtable.grpc.scanner;

import io.grpc.stub.StreamObserver;

/**
 * An interface that allows caller to register an observer of a stream of messages.
 *
 * @param <T> type of message to observe
 */
public interface StreamObservable<T> {
  /**
   * Register an observer.
   *
   * This will initialize the underlying computation, and the observer will be called with the
   * appropriate methods when available.
   *
   * @param observer observer to register
   */
  void observe(StreamObserver<T> observer);
}
