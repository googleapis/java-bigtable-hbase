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
import java.util.Arrays;
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

  public void onFailure(HBaseOperation operation, Throwable throwable) {
    System.out.printf("onFailure: %s: %s", operation, throwable.getMessage());
    mismatchCounter.reportFailure(operation, throwable);
  }

  public void onMismatch(HBaseOperation operation, byte[] primary, byte[] secondary) {
    System.out.printf(
        "onMismatch: %s: %s", operation, String.format("%s != %s", primary, secondary));
    mismatchCounter.reportMismatch(operation, primary, secondary);
    tracer.metricsRecorder.recordReadMismatches(operation, 1);
  }

  public void onLengthMismatch(HBaseOperation operation, int primaryLength, int secondaryLength) {
    System.out.printf(
        "onMismatch: %s: %s",
        operation, String.format("length: %s != %s", primaryLength, secondaryLength));
    mismatchCounter.reportLengthMismatch(operation, primaryLength, secondaryLength);
    tracer.metricsRecorder.recordReadMismatches(operation, 1);
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
      onMismatch(
          HBaseOperation.EXISTS,
          new byte[] {booleanToByte(primary)},
          new byte[] {booleanToByte(secondary)});
    }
    onVerificationFinished();
  }

  @Override
  public void exists(Get request, Throwable throwable) {
    onVerificationStarted();
    onFailure(HBaseOperation.EXISTS, throwable);
    onVerificationFinished();
  }

  @Override
  public void existsAll(List<Get> request, boolean[] primary, boolean[] secondary) {
    onVerificationStarted();
    if (!Arrays.equals(primary, secondary)) {
      byte[] primaryValues = new byte[primary.length];
      byte[] secondaryValues = new byte[secondary.length];

      for (int i = 0; i < primary.length; i++) {
        primaryValues[i] = booleanToByte(primary[i]);
      }

      for (int i = 0; i < secondary.length; i++) {
        secondaryValues[i] = booleanToByte(secondary[i]);
      }
      onMismatch(HBaseOperation.EXISTS_ALL, primaryValues, secondaryValues);
    }
    onVerificationFinished();
  }

  @Override
  public void existsAll(List<Get> request, Throwable throwable) {
    onVerificationStarted();
    onFailure(HBaseOperation.EXISTS_ALL, throwable);
    onVerificationFinished();
  }

  public void get(Get request, Result primary, Result secondary) {
    onVerificationStarted();
    if (!Comparators.resultsEqual(primary, secondary)) {
      onMismatch(HBaseOperation.GET, primary.value(), secondary.value());
    }
    onVerificationFinished();
  }

  @Override
  public void get(Get request, Throwable throwable) {
    onVerificationStarted();
    onFailure(HBaseOperation.GET, throwable);
    onVerificationFinished();
  }

  @Override
  public void get(List<Get> request, Result[] primary, Result[] secondary) {
    onVerificationStarted();
    if (primary.length != secondary.length) {
      onLengthMismatch(HBaseOperation.GET_LIST, primary.length, secondary.length);
      return;
    }

    for (int i = 0; i < primary.length; i++) {
      if (Comparators.resultsEqual(primary[i], secondary[i])) {
        onMismatch(HBaseOperation.GET_LIST, primary[i].value(), secondary[i].value());
      }
    }
    onVerificationFinished();
  }

  @Override
  public void get(List<Get> request, Throwable throwable) {
    onVerificationStarted();
    onFailure(HBaseOperation.GET_LIST, throwable);
    onVerificationFinished();
  }

  @Override
  public void scannerNext(
      Scan request, ScannerResultVerifier verifier, Result primary, Result secondary) {
    verifier.verify(new Result[] {primary}, new Result[] {secondary});
  }

  @Override
  public void scannerNext(Scan request, int entriesAlreadyRead, Throwable throwable) {
    onVerificationStarted();
    onFailure(HBaseOperation.NEXT, throwable);
    onVerificationFinished();
  }

  @Override
  public void scannerNext(
      Scan request, ScannerResultVerifier verifier, Result[] primary, Result[] secondary) {
    verifier.verify(primary, secondary);
  }

  @Override
  public void scannerNext(Scan request, Throwable throwable) {
    onVerificationStarted();
    onFailure(HBaseOperation.NEXT_MULTIPLE, throwable);
    onVerificationFinished();
  }

  @Override
  public void batch(List<Get> request, Result[] primary, Result[] secondary) {
    onVerificationStarted();
    if (primary.length != secondary.length) {
      onLengthMismatch(HBaseOperation.BATCH, primary.length, secondary.length);
      return;
    }

    for (int i = 0; i < primary.length; i++) {
      if (!Comparators.resultsEqual(primary[i], secondary[i])) {
        onMismatch(HBaseOperation.BATCH, primary[i].value(), secondary[i].value());
      }
    }
    onVerificationFinished();
  }

  @Override
  public void batch(List<Get> request, Throwable throwable) {
    onVerificationStarted();
    onFailure(HBaseOperation.BATCH, throwable);
    onVerificationFinished();
  }

  private static byte booleanToByte(boolean b) {
    return (byte) (b ? 1 : 0);
  }

  @Override
  public MismatchDetector.ScannerResultVerifier createScannerResultVerifier(
      Scan request, int maxBufferedResults) {
    return new MemorylessScannerResultVerifier();
  }

  public static class Factory implements MismatchDetector.Factory {

    @Override
    public MismatchDetector create(
        MirroringTracer mirroringTracer, Integer maxLoggedBinaryValueLength) {
      return new TestMismatchDetector(mirroringTracer, maxLoggedBinaryValueLength);
    }
  }

  public class MemorylessScannerResultVerifier implements MismatchDetector.ScannerResultVerifier {
    public MemorylessScannerResultVerifier() {}

    @Override
    public void verify(Result[] primary, Result[] secondary) {
      onVerificationStarted();
      if (primary.length != secondary.length) {
        onLengthMismatch(HBaseOperation.NEXT_MULTIPLE, primary.length, secondary.length);
        return;
      }

      for (int i = 0; i < primary.length; i++) {
        if (!Comparators.resultsEqual(primary[i], secondary[i])) {
          onMismatch(
              HBaseOperation.NEXT_MULTIPLE, valueOrNull(primary[i]), valueOrNull(secondary[i]));
        }
      }
      onVerificationFinished();
    }

    private byte[] valueOrNull(Result result) {
      if (result == null) {
        return null;
      }
      return result.value();
    }

    @Override
    public void flush() {}
  }
}
