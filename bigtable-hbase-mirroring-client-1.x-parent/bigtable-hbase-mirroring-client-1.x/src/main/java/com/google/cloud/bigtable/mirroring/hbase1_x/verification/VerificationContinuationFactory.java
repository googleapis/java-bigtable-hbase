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
package com.google.cloud.bigtable.mirroring.hbase1_x.verification;

import com.google.api.core.InternalApi;
import com.google.common.util.concurrent.FutureCallback;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

@InternalApi("For internal usage only")
public class VerificationContinuationFactory {

  private MismatchDetector mismatchDetector;

  public VerificationContinuationFactory(MismatchDetector mismatchDetector) {
    this.mismatchDetector = mismatchDetector;
  }

  public MismatchDetector getMismatchDetector() {
    return mismatchDetector;
  }

  public FutureCallback<Boolean> exists(final Get request, final boolean expectation) {
    return new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@NullableDecl Boolean secondary) {
        VerificationContinuationFactory.this.mismatchDetector.exists(
            request, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        VerificationContinuationFactory.this.mismatchDetector.exists(request, throwable);
      }
    };
  }

  public FutureCallback<boolean[]> existsAll(final List<Get> request, final boolean[] expectation) {
    return new FutureCallback<boolean[]>() {
      @Override
      public void onSuccess(@NullableDecl boolean[] secondary) {
        VerificationContinuationFactory.this.mismatchDetector.existsAll(
            request, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        VerificationContinuationFactory.this.mismatchDetector.existsAll(request, throwable);
      }
    };
  }

  public FutureCallback<Result> get(final Get request, final Result expectation) {
    return new FutureCallback<Result>() {
      @Override
      public void onSuccess(@NullableDecl Result secondary) {
        VerificationContinuationFactory.this.mismatchDetector.get(request, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        VerificationContinuationFactory.this.mismatchDetector.get(request, throwable);
      }
    };
  }

  public FutureCallback<Result[]> get(final List<Get> request, final Result[] expectation) {
    return new FutureCallback<Result[]>() {
      @Override
      public void onSuccess(@NullableDecl Result[] secondary) {
        VerificationContinuationFactory.this.mismatchDetector.get(request, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        VerificationContinuationFactory.this.mismatchDetector.get(request, throwable);
      }
    };
  }

  public FutureCallback<Result> scannerNext(
      final Scan request, final int entriesAlreadyRead, final Result expectation) {
    return new FutureCallback<Result>() {
      @Override
      public void onSuccess(@NullableDecl Result secondary) {
        VerificationContinuationFactory.this.mismatchDetector.scannerNext(
            request, entriesAlreadyRead, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        VerificationContinuationFactory.this.mismatchDetector.scannerNext(
            request, entriesAlreadyRead, throwable);
      }
    };
  }

  public FutureCallback<Result[]> scannerNext(
      final Scan request,
      final int entriesAlreadyRead,
      final int entriesToRead,
      final Result[] expectation) {
    return new FutureCallback<Result[]>() {
      @Override
      public void onSuccess(@NullableDecl Result[] secondary) {
        VerificationContinuationFactory.this.mismatchDetector.scannerNext(
            request, entriesAlreadyRead, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        VerificationContinuationFactory.this.mismatchDetector.scannerNext(
            request, entriesAlreadyRead, entriesToRead, throwable);
      }
    };
  }
}
