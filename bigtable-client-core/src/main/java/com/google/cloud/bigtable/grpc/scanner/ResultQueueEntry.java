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
package com.google.cloud.bigtable.grpc.scanner;

import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.common.base.Preconditions;

import io.grpc.Status;

import java.io.IOException;


/**
 * An entry in the result queue which may be one of: A data message,
 * a Throwable or a marker indicating end-of-stream.
 * @param <T> The type of messages representing data.
 */
abstract class ResultQueueEntry<T> {

  private static final String EXCEPTION_MESSAGE = "Error in response stream";
  @SuppressWarnings("rawtypes")
  private static final ResultQueueEntry COMPLETION_ENTRY = new ResultQueueEntry() {
    public boolean isCompletionMarker() {
      return true;
    };

    public Object getResponseOrThrow() throws IOException {
      throw new UnsupportedOperationException("getResponseOrThrow()");
    };
  };

  public static <T> ResultQueueEntry<T> newResult(final T response) {
    Preconditions.checkArgument(response != null, "Response may not be null");
    return new ResultQueueEntry<T>(){
      @Override
      public T getResponseOrThrow() throws IOException {
        return response;
      }
    };
  }

  public static <T> ResultQueueEntry<T> newThrowable(final Throwable throwable) {
    Preconditions.checkArgument(throwable != null, "Throwable may not be null");
    final Status status = Status.fromThrowable(throwable);
    return new ResultQueueEntry<T>() {
      @Override
      public T getResponseOrThrow() throws IOException {
        throw new IOExceptionWithStatus(EXCEPTION_MESSAGE, status, throwable);
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static <T> ResultQueueEntry<T> newCompletionMarker() {
    return COMPLETION_ENTRY;
  }

  public boolean isCompletionMarker() {
    return false;
  }

  public abstract T getResponseOrThrow() throws IOException;
}
