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
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.Comparators;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

@InternalApi("For internal usage only")
public class DefaultMismatchDetector implements MismatchDetector {
  public void exists(Get request, boolean primary, boolean secondary) {
    if (primary != secondary) {
      System.out.println("exists mismatch");
    }
  }

  @Override
  public void exists(Get request, Throwable throwable) {
    System.out.println("exists failed");
  }

  @Override
  public void existsAll(List<Get> request, boolean[] primary, boolean[] secondary) {
    if (!Arrays.equals(primary, secondary)) {
      System.out.println("existsAll mismatch");
    }
  }

  @Override
  public void existsAll(List<Get> request, Throwable throwable) {
    System.out.println("existsAll failed");
  }

  public void get(Get request, Result primary, Result secondary) {
    if (!Comparators.resultsEqual(primary, secondary)) {
      System.out.println("get mismatch");
    }
  }

  @Override
  public void get(Get request, Throwable throwable) {
    System.out.println("get failed");
  }

  @Override
  public void get(List<Get> request, Result[] primary, Result[] secondary) {
    if (primary.length != secondary.length) {
      System.out.println("getAll length mismatch");
      return;
    }

    for (int i = 0; i < primary.length; i++) {
      if (Comparators.resultsEqual(primary[i], secondary[i])) {
        System.out.println("getAll mismatch");
      }
    }
  }

  @Override
  public void get(List<Get> request, Throwable throwable) {
    System.out.println("getAll failed");
  }

  @Override
  public void scannerNext(Scan request, int entriesAlreadyRead, Result primary, Result secondary) {
    if (!Comparators.resultsEqual(primary, secondary)) {
      System.out.println("scan() mismatch");
    }
  }

  @Override
  public void scannerNext(Scan request, int entriesAlreadyRead, Throwable throwable) {
    System.out.println("scan() failed");
  }

  @Override
  public void scannerNext(
      Scan request, int entriesAlreadyRead, Result[] primary, Result[] secondary) {
    if (primary.length != secondary.length) {
      System.out.println("scan(i) length mismatch");
      return;
    }

    for (int i = 0; i < primary.length; i++) {
      if (!Comparators.resultsEqual(primary[i], secondary[i])) {
        System.out.println("scan(i) mismatch");
      }
    }
  }

  @Override
  public void scannerNext(
      Scan request, int entriesAlreadyRead, int entriesRequested, Throwable throwable) {
    System.out.println("scan(i) failed");
  }

  @Override
  public void batch(List<Get> request, Result[] primary, Result[] secondary) {
    if (primary.length != secondary.length) {
      System.out.println("batch() length mismatch");
      return;
    }

    for (int i = 0; i < primary.length; i++) {
      if (!Comparators.resultsEqual(primary[i], secondary[i])) {
        System.out.println("batch() mismatch");
      }
    }
  }

  @Override
  public void batch(List<Get> request, Throwable throwable) {
    System.out.println("batch() failed");
  }
}
