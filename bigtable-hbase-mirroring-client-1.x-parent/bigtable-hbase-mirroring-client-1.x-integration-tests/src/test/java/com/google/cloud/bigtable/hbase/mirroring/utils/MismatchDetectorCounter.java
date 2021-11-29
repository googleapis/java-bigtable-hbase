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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MismatchDetectorCounter {
  private int verificationsStartedCounter;
  private int verificationsFinishedCounter;
  private int lengthMismatches;
  private List<Mismatch> mismatches;
  private List<Failure> failures;

  private MismatchDetectorCounter() {
    clearErrors();
  }

  private static MismatchDetectorCounter instance;

  public static synchronized MismatchDetectorCounter getInstance() {
    if (instance == null) {
      instance = new MismatchDetectorCounter();
    }
    return instance;
  }

  public synchronized void reportFailure(HBaseOperation operation, Throwable error) {
    this.failures.add(new Failure(operation, error));
  }

  public synchronized void reportMismatch(
      HBaseOperation operation, byte[] primary, byte[] secondary) {
    this.mismatches.add(new Mismatch(operation, primary, secondary));
  }

  public synchronized void reportLengthMismatch(
      HBaseOperation operation, int primaryLength, int secondaryLength) {
    this.lengthMismatches += 1;
  }

  public synchronized void clearErrors() {
    this.lengthMismatches = 0;
    this.verificationsStartedCounter = 0;
    this.verificationsFinishedCounter = 0;
    this.mismatches = new ArrayList<>();
    this.failures = new ArrayList<>();
  }

  public synchronized int getErrorCount() {
    return this.getLengthMismatchesCount() + this.getFailureCount() + this.getMismatchCount();
  }

  public synchronized int getFailureCount() {
    return this.failures.size();
  }

  public synchronized int getMismatchCount() {
    return this.mismatches.size();
  }

  public synchronized int getLengthMismatchesCount() {
    return this.lengthMismatches;
  }

  public synchronized void onVerificationStarted() {
    this.verificationsStartedCounter++;
  }

  public synchronized void onVerificationFinished() {
    this.verificationsFinishedCounter++;
  }

  public int getVerificationsStartedCounter() {
    return verificationsStartedCounter;
  }

  public int getVerificationsFinishedCounter() {
    return verificationsFinishedCounter;
  }

  public synchronized List<Mismatch> getMismatches() {
    return this.mismatches;
  }

  public static class Mismatch {
    public final byte[] primary;
    public final byte[] secondary;
    public final HBaseOperation operation;

    public Mismatch(HBaseOperation operation, byte[] primary, byte[] secondary) {
      this.primary = primary;
      this.secondary = secondary;
      this.operation = operation;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Mismatch) {
        Mismatch other = (Mismatch) o;
        return this.operation == other.operation
            && Arrays.equals(this.primary, other.primary)
            && Arrays.equals(this.secondary, other.secondary);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return this.operation.hashCode()
          + Arrays.hashCode(this.primary)
          + Arrays.hashCode(this.secondary);
    }
  }

  public static class Failure {
    public final Throwable error;
    public final HBaseOperation operation;

    public Failure(HBaseOperation operation, Throwable throwable) {
      this.operation = operation;
      this.error = throwable;
    }
  }
}
