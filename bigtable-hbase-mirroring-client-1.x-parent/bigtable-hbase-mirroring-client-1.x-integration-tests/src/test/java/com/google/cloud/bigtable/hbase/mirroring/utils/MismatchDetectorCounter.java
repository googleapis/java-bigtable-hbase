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

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;

public class MismatchDetectorCounter {
  private int errorCounter;
  private int verificationsStartedCounter;
  private int verificationsFinishedCounter;
  private List<String> errors;

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

  public synchronized void reportError(String operation, String errorType, String details) {
    this.errors.add(String.format("%s %s %s", operation, errorType, details));
    this.errorCounter += 1;
  }

  public synchronized void clearErrors() {
    this.errorCounter = 0;
    this.verificationsStartedCounter = 0;
    this.verificationsFinishedCounter = 0;
    this.errors = new ArrayList<>();
  }

  public synchronized int getErrorCount() {
    return this.errorCounter;
  }

  public synchronized List<String> getErrors() {
    return this.errors;
  }

  public synchronized String getErrorsAsString() {
    return Joiner.on('\n').join(this.errors);
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
}
