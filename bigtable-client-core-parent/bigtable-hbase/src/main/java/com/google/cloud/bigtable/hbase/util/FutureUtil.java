/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.util;

import com.google.api.core.InternalApi;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Helpers to handle async operations inside the hbase adapter. */
@InternalApi
public class FutureUtil {

  /**
   * Extract the underlying causes for the future's failure.
   *
   * <p>The future's and the callers stacktraces will be merged to ease debugging.
   */
  // This functionality was extracted from HBase 2.x
  public static <T> T unwrap(Future<T> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw (IOException) new InterruptedIOException().initCause(e);
    } catch (ExecutionException e) {
      throw rethrow(e.getCause());
    }
  }

  private static IOException rethrow(Throwable error) throws IOException {
    if (error instanceof IOException) {
      setStackTrace(error);
      throw (IOException) error;
    } else if (error instanceof RuntimeException) {
      setStackTrace(error);
      throw (RuntimeException) error;
    } else if (error instanceof Error) {
      setStackTrace(error);
      throw (Error) error;
    } else {
      throw new IOException(error);
    }
  }

  private static void setStackTrace(Throwable error) {
    StackTraceElement[] localStackTrace = Thread.currentThread().getStackTrace();
    StackTraceElement[] originalStackTrace = error.getStackTrace();
    StackTraceElement[] newStackTrace =
        new StackTraceElement[localStackTrace.length + originalStackTrace.length + 1];
    System.arraycopy(localStackTrace, 0, newStackTrace, 0, localStackTrace.length);
    newStackTrace[localStackTrace.length] =
        new StackTraceElement("--------Future", "get--------", null, -1);
    System.arraycopy(
        originalStackTrace,
        0,
        newStackTrace,
        localStackTrace.length + 1,
        originalStackTrace.length);
    error.setStackTrace(newStackTrace);
  }
}
