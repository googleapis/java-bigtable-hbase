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
package com.google.cloud.bigtable.scanner;

import com.google.cloud.bigtable.grpc.IOExceptionWithStatus;
import com.google.common.base.Preconditions;

import io.grpc.StatusRuntimeException;

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
      if (throwable instanceof StatusRuntimeException) {
        throw new IOExceptionWithStatus(EXCEPTION_MESSAGE, (StatusRuntimeException) throwable);
      }
      throw new IOException(EXCEPTION_MESSAGE, throwable);
    }
    if (isCompletionMarker()) {
      throw new IOException("Attempt to interpret a result stream completion marker as a result");
    }
    return response;
  }
}
