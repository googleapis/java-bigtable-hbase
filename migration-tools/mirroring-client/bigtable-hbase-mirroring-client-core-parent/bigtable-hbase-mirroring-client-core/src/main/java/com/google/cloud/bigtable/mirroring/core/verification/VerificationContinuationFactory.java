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
package com.google.cloud.bigtable.mirroring.core.verification;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.core.asyncwrappers.AsyncResultScannerWrapper.AsyncScannerExceptionWithContext;
import com.google.cloud.bigtable.mirroring.core.asyncwrappers.AsyncResultScannerWrapper.AsyncScannerVerificationPayload;
import com.google.cloud.bigtable.mirroring.core.utils.Logger;
import com.google.common.util.concurrent.FutureCallback;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

@InternalApi("For internal usage only")
public class VerificationContinuationFactory {
  private MismatchDetector mismatchDetector;
  private static final Logger Log = new Logger(VerificationContinuationFactory.class);

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
        Log.trace("verification onSuccess exists(Get)");
        VerificationContinuationFactory.this.mismatchDetector.exists(
            request, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        Log.trace("verification onFailure exists(Get)");
        VerificationContinuationFactory.this.mismatchDetector.exists(request, throwable);
      }
    };
  }

  public FutureCallback<boolean[]> existsAll(final List<Get> request, final boolean[] expectation) {
    return new FutureCallback<boolean[]>() {
      @Override
      public void onSuccess(@NullableDecl boolean[] secondary) {
        Log.trace("verification onSuccess existsAll(List<Get>)");
        VerificationContinuationFactory.this.mismatchDetector.existsAll(
            request, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        Log.trace("verification onFailure existsAll(List<Get>)");
        VerificationContinuationFactory.this.mismatchDetector.existsAll(request, throwable);
      }
    };
  }

  public FutureCallback<Result> get(final Get request, final Result expectation) {
    return new FutureCallback<Result>() {
      @Override
      public void onSuccess(@NullableDecl Result secondary) {
        Log.trace("verification onSuccess get(Get)");
        VerificationContinuationFactory.this.mismatchDetector.get(request, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        Log.trace("verification onFailure get(Get)");
        VerificationContinuationFactory.this.mismatchDetector.get(request, throwable);
      }
    };
  }

  public FutureCallback<Result[]> get(final List<Get> request, final Result[] expectation) {
    return new FutureCallback<Result[]>() {
      @Override
      public void onSuccess(@NullableDecl Result[] secondary) {
        Log.trace("verification onSuccess get(List<Get>)");
        VerificationContinuationFactory.this.mismatchDetector.get(request, expectation, secondary);
      }

      @Override
      public void onFailure(Throwable throwable) {
        Log.trace("verification onFailure get(List<Get>)");
        VerificationContinuationFactory.this.mismatchDetector.get(request, throwable);
      }
    };
  }

  private Result firstOrNull(Result[] results) {
    if (results == null) {
      return null;
    }
    if (results.length == 0) {
      return null;
    }
    return results[0];
  }

  public FutureCallback<Void> scannerNext(
      final Object verificationLock,
      final ConcurrentLinkedQueue<AsyncScannerVerificationPayload> resultQueue,
      final MismatchDetector.ScannerResultVerifier unmatched) {
    return new FutureCallback<Void>() {
      @Override
      public void onSuccess(@NullableDecl Void ignored) {
        synchronized (verificationLock) {
          AsyncScannerVerificationPayload results = resultQueue.remove();
          Log.trace("verification onSuccess scannerNext(Scan, int)");
          Scan request = results.context.scan;
          if (results.context.singleNext) {
            VerificationContinuationFactory.this.mismatchDetector.scannerNext(
                request,
                unmatched,
                firstOrNull(results.context.result),
                firstOrNull(results.secondary));
          } else {
            VerificationContinuationFactory.this.mismatchDetector.scannerNext(
                request, unmatched, results.context.result, results.secondary);
          }
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        Log.trace("verification onFailure scannerNext(Scan, int)");
        if (throwable instanceof AsyncScannerExceptionWithContext) {
          AsyncScannerExceptionWithContext exceptionWithContext =
              (AsyncScannerExceptionWithContext) throwable;
          Scan request = exceptionWithContext.context.scan;
          if (exceptionWithContext.context.singleNext) {
            VerificationContinuationFactory.this.mismatchDetector.scannerNext(
                request, throwable.getCause());
          } else {
            VerificationContinuationFactory.this.mismatchDetector.scannerNext(
                request, exceptionWithContext.context.numRequests, throwable.getCause());
          }
        } else {
          Log.error(
              "scannerNext.onFailure received an exception that is not a AsyncScannerExceptionWithContext: %s",
              throwable);
        }
      }
    };
  }
}
