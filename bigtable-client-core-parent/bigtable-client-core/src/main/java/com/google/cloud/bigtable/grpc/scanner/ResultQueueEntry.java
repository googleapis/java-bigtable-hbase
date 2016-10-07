/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
import java.util.Objects;


/**
 * An entry in the result queue which may be one of: A data message,
 * a Throwable or a marker indicating end-of-stream.
 * @param <T> The type of messages representing data.
 */
abstract class ResultQueueEntry<T> {

  public enum Type {
    Data,
    Exception,
    CompletionMarker;
  }

  /**
   * <p>fromResponse.</p>
   *
   * @param response a T object.
   * @param <T> a T object.
   * @return a {@link com.google.cloud.bigtable.grpc.scanner.ResultQueueEntry} object.
   */
  public static <T> ResultQueueEntry<T> fromResponse(T response) {
    Preconditions.checkArgument(response != null, "Response may not be null");
    return new ResponseResultQueueEntry<T>(response);
  }

  /**
   * <p>fromThrowable.</p>
   *
   * @param throwable a {@link java.lang.Throwable} object.
   * @param <T> a T object.
   * @return a {@link com.google.cloud.bigtable.grpc.scanner.ResultQueueEntry} object.
   */
  public static <T> ResultQueueEntry<T> fromThrowable(Throwable throwable) {
    Preconditions.checkArgument(throwable != null, "Throwable may not be null");
    return new ExceptionResultQueueEntry<T>(throwable);
  }

  /**
   * <p>completionMarker.</p>
   *
   * @param <T> a T object.
   * @return a {@link com.google.cloud.bigtable.grpc.scanner.ResultQueueEntry} object.
   */
  @SuppressWarnings("unchecked")
  public static <T> ResultQueueEntry<T> completionMarker() {
    return COMPLETION_ENTRY;
  }

  private static final class ExceptionResultQueueEntry<T> extends ResultQueueEntry<T> {
    private static final String EXCEPTION_MESSAGE = "Error in response stream";

    private final Throwable throwable;

    private ExceptionResultQueueEntry(Throwable throwable) {
      super(Type.Exception);
      this.throwable = throwable;
    }

    @Override
    public T getResponseOrThrow() throws IOException {
      throw new IOExceptionWithStatus(
          EXCEPTION_MESSAGE, throwable, Status.fromThrowable(throwable));
    }

    @Override
    public boolean equals(Object obj) {
      ExceptionResultQueueEntry<T> other = (ExceptionResultQueueEntry<T>) toResultQueueEntryForEquals(obj);
      return other != null && Objects.equals(throwable, other.throwable);
    }
  }

  private static final class ResponseResultQueueEntry<T> extends ResultQueueEntry<T> {
    private final T response;

    private ResponseResultQueueEntry(T response) {
      super(Type.Data);
      this.response = response;
    }

    @Override
    public T getResponseOrThrow() throws IOException {
      return response;
    }

    @Override
    public boolean equals(Object obj) {
      ResponseResultQueueEntry<T> other = (ResponseResultQueueEntry<T>) toResultQueueEntryForEquals(obj);
      return other != null && Objects.equals(response, other.response);
    }
  }

  @SuppressWarnings("rawtypes")
  private static final ResultQueueEntry COMPLETION_ENTRY =
      new ResultQueueEntry(Type.CompletionMarker) {
        @Override
        public Object getResponseOrThrow() throws IOException {
          throw new IOException(
              "Attempt to interpret a result stream completion marker as a result");
        }

        @Override
        public boolean equals(Object obj) {
          // toResultQueueEntry will return null if the type field is different between this and
          // obj.
          return toResultQueueEntryForEquals(obj) != null;
        }
      };

  protected final Type type;

  public ResultQueueEntry(Type type) {
    this.type = type;
  }

  /**
   * <p>isCompletionMarker.</p>
   *
   * @return a boolean.
   */
  public Type getType() {
    return type;
  }

  /**
   * <p>getResponseOrThrow.</p>
   *
   * @return a T object.
   * @throws java.io.IOException if any.
   */
  public abstract T getResponseOrThrow() throws IOException;

  /**
   * This is a utility function for checking equality between this and another ResultQueueEntry.
   * @param obj the object to compare to
   * @return a reference to ResultQueueEntry if obj is: 1) not null, 2) a ResultQueueEntry, 3) has
   *         the same type as this.
   */
  @SuppressWarnings("unchecked")
  protected ResultQueueEntry<T> toResultQueueEntryForEquals(Object obj) {
    if (!(obj instanceof ResultQueueEntry) || obj == null) {
      return null;
    }
    ResultQueueEntry<T> other = (ResultQueueEntry<T>) obj;
    return type == other.type && getClass() == other.getClass() ? other : null;
  }
}
