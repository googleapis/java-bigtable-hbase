/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils;

import com.google.api.core.InternalApi;
import java.io.IOException;

/**
 * Accumulates exceptions caught during mirroring operations. {@link
 * AccumulatedExceptions#rethrowIfCaptured()} will rethrow first exception added to this accumulator
 * with others attached as suppressed exceptions (see {@link Exception#addSuppressed(Throwable)}),
 * if any. Handles only {@link IOException} and {@link RuntimeException} because only those can be
 * thrown by HBase operations.
 */
@InternalApi
public class AccumulatedExceptions {
  Exception exception = null;

  public void add(IOException exception) {
    if (this.exception == null) {
      this.exception = exception;
    } else {
      this.exception.addSuppressed(exception);
    }
  }

  public void add(RuntimeException exception) {
    if (this.exception == null) {
      this.exception = exception;
    } else {
      this.exception.addSuppressed(exception);
    }
  }

  public void rethrowIfCaptured() throws IOException {
    if (this.exception == null) {
      return;
    }

    if (this.exception instanceof IOException) {
      throw (IOException) this.exception;
    } else {
      assert this.exception instanceof RuntimeException;
      throw (RuntimeException) this.exception;
    }
  }

  public void rethrowAsRuntimeExceptionIfCaptured() {
    try {
      this.rethrowIfCaptured();
    } catch (IOException e) {
      assert false;
      throw new RuntimeException(e);
    }
  }
}
