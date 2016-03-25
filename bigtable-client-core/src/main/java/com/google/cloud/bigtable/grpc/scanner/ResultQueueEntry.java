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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import io.grpc.Status;

import java.io.IOException;


/**
 * An entry in the result queue which may be one of: A data message,
 * a Throwable or a marker indicating end-of-stream.
 * @param <T> The type of messages representing data.
 */
abstract class ResultQueueEntry<T> {

  public static <T> ResultQueueEntry<T> fromResponse(T response) {
    Preconditions.checkArgument(response != null, "Response may not be null");
    return new ResponseResultQueueEntry<T>(response);
  }

  public static <T> ResultQueueEntry<T> fromThrowable(Throwable throwable) {
    Preconditions.checkArgument(throwable != null, "Throwable may not be null");
    return new ExceptionResultQueueEntry<T>(throwable);
  }

  @SuppressWarnings("unchecked")
  public static <T> ResultQueueEntry<T> completionMarker() {
    return COMPLETION_ENTRY;
  }

  private static final class ExceptionResultQueueEntry<T> extends ResultQueueEntry<T> {
    private static final String EXCEPTION_MESSAGE = "Error in response stream";

    private final Throwable throwable;

    private ExceptionResultQueueEntry(Throwable throwable) {
      this.throwable = throwable;
    }

    @Override
    public boolean isCompletionMarker() {
      return false;
    }

    @Override
    public T getResponseOrThrow() throws IOException {
      throw new IOExceptionWithStatus(
          EXCEPTION_MESSAGE, throwable, Status.fromThrowable(throwable));
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object obj) {
      if (!(obj instanceof ExceptionResultQueueEntry) || obj == null){
        return false;
      }
      ExceptionResultQueueEntry other = (ExceptionResultQueueEntry) obj;
      return Objects.equal(throwable, other.throwable);
    }
  }

  private static final class ResponseResultQueueEntry<T> extends ResultQueueEntry<T> {
    private final T response;

    private ResponseResultQueueEntry(T response) {
      this.response = response;
    }

    @Override
    public boolean isCompletionMarker() {
      return false;
    }

    @Override
    public T getResponseOrThrow() throws IOException {
      return response;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public boolean equals(Object obj) {
      if (!(obj instanceof ResponseResultQueueEntry) || obj == null){
        return false;
      }
      ResponseResultQueueEntry other = (ResponseResultQueueEntry) obj;
      return Objects.equal(response, other.response);
    }
  }

  @SuppressWarnings("rawtypes")
  private static final ResultQueueEntry COMPLETION_ENTRY = new ResultQueueEntry(){
    @Override
    public boolean isCompletionMarker() {
      return true;
    }

    @Override
    public Object getResponseOrThrow() throws IOException {
      throw new IOException("Attempt to interpret a result stream completion marker as a result");
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ResultQueueEntry) || obj == null){
        return false;
      }
      ResultQueueEntry other = (ResultQueueEntry) obj;
      return isCompletionMarker() == other.isCompletionMarker();
    }
  };

  public abstract boolean isCompletionMarker();

  public abstract T getResponseOrThrow() throws IOException;
}
