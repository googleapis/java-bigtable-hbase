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

import static com.google.cloud.bigtable.mirroring.hbase1_x.verification.DefaultMismatchDetector.LazyBytesHexlifier.listOfHexRows;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Comparators;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Logger;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringMetricsRecorder;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

@InternalApi("For internal usage only")
public class DefaultMismatchDetector implements MismatchDetector {
  private final int maxValueBytesLogged;
  private static final Logger Log = new Logger(DefaultMismatchDetector.class);
  private final MirroringMetricsRecorder metricsRecorder;

  public DefaultMismatchDetector(MirroringTracer mirroringTracer, Integer maxValueBytesLogged) {
    this.metricsRecorder = mirroringTracer.metricsRecorder;
    this.maxValueBytesLogged = maxValueBytesLogged;
  }

  public void exists(Get request, boolean primary, boolean secondary) {
    if (primary == secondary) {
      this.metricsRecorder.recordReadMatches(HBaseOperation.EXISTS, 1);
    } else {
      Log.debug(
          "exists(row=%s) mismatch: (%b, %b)",
          new LazyBytesHexlifier(request.getRow(), maxValueBytesLogged), primary, secondary);
      this.metricsRecorder.recordReadMismatches(HBaseOperation.EXISTS, 1);
    }
  }

  @Override
  public void exists(Get request, Throwable throwable) {
    Log.debug(
        "exists(row=%s) failed: (throwable=%s)",
        new LazyBytesHexlifier(request.getRow(), maxValueBytesLogged), throwable);
  }

  @Override
  public void existsAll(List<Get> request, boolean[] primary, boolean[] secondary) {
    if (!Arrays.equals(primary, secondary)) {
      int mismatches = 0;
      for (int i = 0; i < primary.length; i++) {
        if (primary[i] != secondary[i]) {
          Log.debug(
              "existsAll(row=%s) mismatch: (%b, %b)",
              new LazyBytesHexlifier(request.get(i).getRow(), maxValueBytesLogged),
              primary[i],
              secondary[i]);
          mismatches++;
        }
      }
      if (mismatches != primary.length) {
        this.metricsRecorder.recordReadMatches(
            HBaseOperation.EXISTS_ALL, primary.length - mismatches);
      }
      this.metricsRecorder.recordReadMismatches(HBaseOperation.EXISTS_ALL, mismatches);
    }
  }

  @Override
  public void existsAll(List<Get> request, Throwable throwable) {
    Log.debug(
        "existsAll(rows=%s) failed: (throwable=%s)",
        listOfHexRows(request, maxValueBytesLogged), throwable);
  }

  public void get(Get request, Result primary, Result secondary) {
    if (Comparators.resultsEqual(primary, secondary)) {
      this.metricsRecorder.recordReadMatches(HBaseOperation.GET, 1);
    } else {
      Log.debug(
          "get(row=%s) mismatch: (%s, %s)",
          new LazyBytesHexlifier(request.getRow(), maxValueBytesLogged),
          new LazyBytesHexlifier(getResultValue(primary), maxValueBytesLogged),
          new LazyBytesHexlifier(getResultValue(secondary), maxValueBytesLogged));
      this.metricsRecorder.recordReadMismatches(HBaseOperation.GET, 1);
    }
  }

  @Override
  public void get(Get request, Throwable throwable) {
    Log.debug(
        "get(row=%s) failed: (throwable=%s)",
        new LazyBytesHexlifier(request.getRow(), maxValueBytesLogged), throwable);
  }

  @Override
  public void get(List<Get> request, Result[] primary, Result[] secondary) {
    verifyResults(primary, secondary, "get", HBaseOperation.GET_LIST);
  }

  @Override
  public void get(List<Get> request, Throwable throwable) {
    Log.debug(
        "get(rows=%s) failed: (throwable=%s)",
        listOfHexRows(request, maxValueBytesLogged), throwable);
  }

  @Override
  public void scannerNext(Scan request, int entriesAlreadyRead, Result primary, Result secondary) {
    if (Comparators.resultsEqual(primary, secondary)) {
      this.metricsRecorder.recordReadMatches(HBaseOperation.NEXT, 1);
    } else {
      Log.debug(
          "scan[id=%s, entriesRead=%d] mismatch: (%s, %s)",
          request.getId(),
          entriesAlreadyRead,
          new LazyBytesHexlifier(getResultValue(primary), maxValueBytesLogged),
          new LazyBytesHexlifier(getResultValue(secondary), maxValueBytesLogged));
      this.metricsRecorder.recordReadMismatches(HBaseOperation.NEXT, 1);
    }
  }

  @Override
  public void scannerNext(Scan request, int entriesAlreadyRead, Throwable throwable) {
    Log.debug(
        "scan[id=%s, entriesRead=%d] failed: (throwable=%s)",
        request.getId(), entriesAlreadyRead, throwable);
  }

  @Override
  public void scannerNext(
      Scan request, int entriesAlreadyRead, Result[] primary, Result[] secondary) {
    verifyResults(
        primary,
        secondary,
        String.format("scan[id=%s]", request.getId()),
        HBaseOperation.NEXT_MULTIPLE);
  }

  @Override
  public void scannerNext(
      Scan request, int entriesAlreadyRead, int entriesRequested, Throwable throwable) {
    Log.debug(
        "scan[id=%s, entriesRead=%d, entriesRequested=%d] failed: (throwable=%s)",
        request.getId(), entriesAlreadyRead, entriesRequested, throwable);
  }

  @Override
  public void batch(List<Get> request, Result[] primary, Result[] secondary) {
    verifyResults(primary, secondary, "batch", HBaseOperation.BATCH);
  }

  @Override
  public void batch(List<Get> request, Throwable throwable) {
    Log.debug(
        "batch(rows=%s) failed: (throwable=%s)",
        listOfHexRows(request, maxValueBytesLogged), throwable);
  }

  private void verifyResults(
      Result[] primary, Result[] secondary, String operationName, HBaseOperation operation) {
    int minLength = Math.min(primary.length, secondary.length);
    int maxLength = Math.max(primary.length, secondary.length);
    int errors = maxLength - minLength;
    int matches = 0;
    for (int i = 0; i < minLength; i++) {
      if (Comparators.resultsEqual(primary[i], secondary[i])) {
        matches++;
      } else {
        Log.debug(
            "%s(row=%s) mismatch: (%s, %s)",
            operationName,
            new LazyBytesHexlifier(getResultRow(primary[i]), maxValueBytesLogged),
            new LazyBytesHexlifier(getResultValue(primary[i]), maxValueBytesLogged),
            new LazyBytesHexlifier(getResultValue(secondary[i]), maxValueBytesLogged));
        errors++;
      }
    }
    if (matches > 0) {
      this.metricsRecorder.recordReadMatches(operation, matches);
    }
    if (errors > 0) {
      this.metricsRecorder.recordReadMismatches(operation, errors);
    }
  }

  private byte[] getResultValue(Result primary) {
    if (primary == null) {
      return null;
    }
    return primary.value();
  }

  private byte[] getResultRow(Result result) {
    if (result == null) {
      return null;
    }
    return result.getRow();
  }

  // Used for logging. Overrides toString() in order to be as lazy as possible.
  // Adapted from Apache Common Codec's Hex.
  public static class LazyBytesHexlifier {
    private static final char[] DIGITS = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    public static List<LazyBytesHexlifier> listOfHexRows(List<Get> gets, int maxBytesPrinted) {
      List<LazyBytesHexlifier> out = new ArrayList<>(gets.size());
      for (Get get : gets) {
        out.add(new LazyBytesHexlifier(get.getRow(), maxBytesPrinted));
      }
      return out;
    }

    private final byte[] bytes;
    private final int maxBytesPrinted;

    public LazyBytesHexlifier(byte[] bytes, int maxBytesPrinted) {
      this.bytes = bytes;
      this.maxBytesPrinted = maxBytesPrinted;
    }

    private void bytesToHex(
        final char[] out, final int outOffset, final int bytesOffset, final int bytesLength) {
      for (int i = bytesOffset, j = outOffset; i < bytesOffset + bytesLength; i++) {
        out[j++] = DIGITS[(0xF0 & this.bytes[i]) >>> 4];
        out[j++] = DIGITS[0x0F & this.bytes[i]];
      }
    }

    @Override
    public String toString() {
      if (this.bytes == null) {
        return "null";
      }

      int bytesToPrint = Math.min(this.bytes.length, maxBytesPrinted);
      if (bytesToPrint <= 0) {
        return "";
      }
      boolean skipSomeBytes = bytesToPrint != this.bytes.length;
      char[] out;
      if (skipSomeBytes) {
        int numEndBytes = bytesToPrint / 2;
        int numStartBytes = bytesToPrint - numEndBytes;
        int numDots = 3;

        int startDotsIdx = 2 * numStartBytes;
        int endDotsIdx = 2 * numStartBytes + numDots;

        out = new char[numDots + (bytesToPrint << 1)];

        bytesToHex(out, 0, 0, numStartBytes);
        for (int i = startDotsIdx; i < endDotsIdx; i++) {
          out[i] = '.';
        }
        bytesToHex(out, endDotsIdx, this.bytes.length - numEndBytes, numEndBytes);
      } else {
        out = new char[bytesToPrint << 1];
        bytesToHex(out, 0, 0, bytesToPrint);
      }
      return new String(out);
    }
  }

  public static class Factory implements MismatchDetector.Factory {
    @Override
    public MismatchDetector create(MirroringTracer mirroringTracer, Integer maxValueBytesLogged) {
      return new DefaultMismatchDetector(mirroringTracer, maxValueBytesLogged);
    }
  }
}
