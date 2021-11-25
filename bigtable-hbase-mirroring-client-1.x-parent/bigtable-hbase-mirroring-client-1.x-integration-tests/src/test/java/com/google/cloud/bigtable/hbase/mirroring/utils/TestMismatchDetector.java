/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Comparators;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public class TestMismatchDetector implements MismatchDetector {
  private final MismatchDetectorCounter mismatchCounter = MismatchDetectorCounter.getInstance();
  private final MirroringTracer tracer;

  public TestMismatchDetector(MirroringTracer tracer, Integer ignored) {
    this.tracer = tracer;
  }

  public void onError(HBaseOperation operation, String errorType, String details) {
    System.out.printf("onError: %s: %s, %s", operation, errorType, details);
    mismatchCounter.reportError(operation.getString(), errorType, details);
    if (errorType.equals("mismatch")) {
      tracer.metricsRecorder.recordReadMismatches(operation, 1);
    }
  }

  public void onVerificationStarted() {
    mismatchCounter.onVerificationStarted();
  }

  public void onVerificationFinished() {
    mismatchCounter.onVerificationFinished();
  }

  public void exists(Get request, boolean primary, boolean secondary) {
    onVerificationStarted();
    if (primary != secondary) {
      onError(HBaseOperation.EXISTS, "mismatch", String.format("%s != %s", primary, secondary));
    }
    onVerificationFinished();
  }

  @Override
  public void exists(Get request, Throwable throwable) {
    onVerificationStarted();
    onError(HBaseOperation.EXISTS, "failure", throwable.getMessage());
    onVerificationFinished();
  }

  @Override
  public void existsAll(List<Get> request, boolean[] primary, boolean[] secondary) {
    onVerificationStarted();
    for (int i = 0; i < primary.length; i++) {
      if (primary[i] != secondary[i]) {
        onError(
            HBaseOperation.EXISTS_ALL,
            "mismatch",
            String.format("%s != %s", primary[i], secondary[i]));
      }
    }
    onVerificationFinished();
  }

  @Override
  public void existsAll(List<Get> request, Throwable throwable) {
    onVerificationStarted();
    onError(HBaseOperation.EXISTS_ALL, "failure", throwable.getMessage());
    onVerificationFinished();
  }

  public void get(Get request, Result primary, Result secondary) {
    onVerificationStarted();
    if (!Comparators.resultsEqual(primary, secondary)) {
      onError(HBaseOperation.GET, "mismatch", String.format("%s != %s", primary, secondary));
    }
    onVerificationFinished();
  }

  @Override
  public void get(Get request, Throwable throwable) {
    onVerificationStarted();
    onError(HBaseOperation.GET, "failure", throwable.getMessage());
    onVerificationFinished();
  }

  @Override
  public void get(List<Get> request, Result[] primary, Result[] secondary) {
    onVerificationStarted();
    if (primary.length != secondary.length) {
      onError(
          HBaseOperation.GET_LIST,
          "length mismatch",
          String.format("%s != %s", primary.length, secondary.length));
      return;
    }

    for (int i = 0; i < primary.length; i++) {
      if (Comparators.resultsEqual(primary[i], secondary[i])) {
        onError(
            HBaseOperation.GET_LIST,
            "mismatch",
            String.format("(index=%s), %s != %s", i, primary[i], secondary[i]));
      }
    }
    onVerificationFinished();
  }

  @Override
  public void get(List<Get> request, Throwable throwable) {
    onVerificationStarted();
    onError(HBaseOperation.GET_LIST, "failed", throwable.getMessage());
    onVerificationFinished();
  }

  @Override
  public void scannerNext(Scan request, int entriesAlreadyRead, Result primary, Result secondary) {
    onVerificationStarted();
    if (!Comparators.resultsEqual(primary, secondary)) {
      onError(HBaseOperation.NEXT, "mismatch", String.format("%s != %s", primary, secondary));
    }
    onVerificationFinished();
  }

  @Override
  public void scannerNext(Scan request, int entriesAlreadyRead, Throwable throwable) {
    onVerificationStarted();
    onError(HBaseOperation.NEXT, "failed", throwable.getMessage());
  }

  @Override
  public void scannerNext(
      Scan request, int entriesAlreadyRead, Result[] primary, Result[] secondary) {
    onVerificationStarted();
    if (primary.length != secondary.length) {
      onError(
          HBaseOperation.NEXT_MULTIPLE,
          "length mismatch",
          String.format("%s != %s", primary.length, secondary.length));
      return;
    }

    for (int i = 0; i < primary.length; i++) {
      if (!Comparators.resultsEqual(primary[i], secondary[i])) {
        onError(
            HBaseOperation.NEXT_MULTIPLE,
            "mismatch",
            String.format(
                "(index=%s), %s != %s", entriesAlreadyRead + i, primary[i], secondary[i]));
      }
    }
    onVerificationFinished();
  }

  @Override
  public void scannerNext(
      Scan request, int entriesAlreadyRead, int entriesRequested, Throwable throwable) {
    onVerificationStarted();
    onError(HBaseOperation.NEXT_MULTIPLE, "failure", throwable.getMessage());
    onVerificationFinished();
  }

  @Override
  public void batch(List<Get> request, Result[] primary, Result[] secondary) {
    onVerificationStarted();
    if (primary.length != secondary.length) {
      onError(
          HBaseOperation.BATCH,
          "length mismatch",
          String.format("%s != %s", primary.length, secondary.length));
      return;
    }

    for (int i = 0; i < primary.length; i++) {
      if (!Comparators.resultsEqual(primary[i], secondary[i])) {
        onError(
            HBaseOperation.BATCH,
            "mismatch",
            String.format("(index=%s), %s != %s", i, primary[i], secondary[i]));
      }
    }
    onVerificationFinished();
  }

  @Override
  public void batch(List<Get> request, Throwable throwable) {
    onVerificationStarted();
    onError(HBaseOperation.BATCH, "failed", throwable.getMessage());
    onVerificationFinished();
  }
}
