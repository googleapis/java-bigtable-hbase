package com.google.cloud.bigtable.grpc;

import com.google.common.base.Preconditions;

import io.grpc.Status.OperationRuntimeException;

import java.io.IOException;


/**
 * An entry in the result queue which may be one of: A data message,
 * a Throwable or a marker indicating end-of-stream.
 * @param <T> The type of messages representing data.
 */
class ResultQueueEntry<T> {

  private static final String EXCEPTION_MESSAGE = "Error in response stream";
  protected final Throwable throwable;
  protected final T response;
  protected final boolean isComplete;

  public static <T> ResultQueueEntry<T> newResult(T response) {
    Preconditions.checkArgument(response != null, "Response may not be null");
    return new ResultQueueEntry<>(null, response, false);
  }

  public static <T> ResultQueueEntry<T> newThrowable(Throwable throwable) {
    Preconditions.checkArgument(throwable != null, "Throwable may not be null");
    return new ResultQueueEntry<>(throwable, null, false);
  }

  public static <T> ResultQueueEntry<T> newCompletionMarker() {
    return new ResultQueueEntry<>(null, null, true);
  }

  protected ResultQueueEntry(Throwable throwable, T response, boolean isCompletionMarker) {
    this.throwable = throwable;
    this.response = response;
    this.isComplete = isCompletionMarker;
  }

  public boolean isCompletionMarker() {
    return isComplete;
  }

  public T getResponseOrThrow() throws IOException {
    if (throwable != null) {
      if (throwable instanceof OperationRuntimeException) {
        throw new IOExceptionWithStatus(EXCEPTION_MESSAGE, (OperationRuntimeException) throwable);
      }
      throw new IOException(EXCEPTION_MESSAGE, throwable);
    }
    if (isCompletionMarker()) {
      throw new IOException("Attempt to interpret a result stream completion marker as a result");
    }
    return response;
  }
}
