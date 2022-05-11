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

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.FutureCallback;
import io.opencensus.common.Scope;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class BatchHelpers {
  public static FutureCallback<Void> createBatchVerificationCallback(
      final FailedSuccessfulSplit<?> failedAndSuccessfulPrimaryOperations,
      final ReadWriteSplit<?, Result> successfulPrimaryReadsAndWrites,
      final Object[] secondaryResults,
      final MismatchDetector mismatchDetector,
      final SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      final Predicate<Object> resultIsFaultyPredicate,
      final MirroringTracer mirroringTracer) {
    return new FutureCallback<Void>() {
      @Override
      public void onSuccess(@NullableDecl Void t) {
        // Batch is successful - all results are correct.
        List<? extends Row> secondaryOperations =
            failedAndSuccessfulPrimaryOperations.successfulOperations;

        final FailedSuccessfulSplit<?> secondaryFailedAndSuccessfulOperations =
            new FailedSuccessfulSplit<>(
                secondaryOperations, secondaryResults, resultIsFaultyPredicate);

        final ReadWriteSplit<?, Result> successfulSecondaryReadsAndWrites =
            new ReadWriteSplit<>(
                secondaryFailedAndSuccessfulOperations.successfulOperations,
                secondaryFailedAndSuccessfulOperations.successfulResults,
                Result.class);

        if (successfulSecondaryReadsAndWrites.readOperations.size() > 0) {
          try (Scope scope = mirroringTracer.spanFactory.verificationScope()) {
            mismatchDetector.batch(
                successfulSecondaryReadsAndWrites.readOperations,
                successfulPrimaryReadsAndWrites.readResults,
                successfulSecondaryReadsAndWrites.readResults);
          }
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        // Batch has thrown - partial results might be available.
        List<? extends Row> secondaryOperations =
            failedAndSuccessfulPrimaryOperations.successfulOperations;

        final FailedSuccessfulSplit<?> secondaryFailedAndSuccessfulOperations =
            new FailedSuccessfulSplit<>(
                secondaryOperations, secondaryResults, resultIsFaultyPredicate);

        final ReadWriteSplit<?, Result> successfulSecondaryReadsAndWrites =
            new ReadWriteSplit<>(
                secondaryFailedAndSuccessfulOperations.successfulOperations,
                secondaryFailedAndSuccessfulOperations.successfulResults,
                Result.class);

        final ReadWriteSplit<?, ?> failedSecondaryReadsAndWrites =
            new ReadWriteSplit<>(
                secondaryFailedAndSuccessfulOperations.failedOperations,
                secondaryFailedAndSuccessfulOperations.failedResults,
                Object.class);

        if (failedSecondaryReadsAndWrites.writeOperations.size() > 0) {
          consumeWriteErrors(
              failedSecondaryReadsAndWrites.writeOperations,
              failedSecondaryReadsAndWrites.writeResults);
        }

        if (successfulSecondaryReadsAndWrites.readOperations.size() > 0
            || failedSecondaryReadsAndWrites.readOperations.size() > 0) {
          // Some of the reads in this batch might have been not successful.
          // We want to verify successful reads and report the others.

          // We are using `secondaryResults` to select indices of operations that were successful.
          // Using those indices we select Get operations that have results from both primary and
          // secondary database, and pass them to `mismatchDetector.batch()`.
          // We also gather failed gets to pass them to `batchGetFailure`.
          MatchingSuccessfulReadsResults matchingSuccessfulReads =
              selectMatchingSuccessfulReads(
                  secondaryOperations,
                  failedAndSuccessfulPrimaryOperations.successfulResults,
                  secondaryResults,
                  resultIsFaultyPredicate);

          try (Scope scope = mirroringTracer.spanFactory.verificationScope()) {
            if (!matchingSuccessfulReads.successfulReads.isEmpty()) {
              mismatchDetector.batch(
                  successfulSecondaryReadsAndWrites.readOperations,
                  matchingSuccessfulReads.primaryResults,
                  matchingSuccessfulReads.secondaryResults);
            }

            if (!matchingSuccessfulReads.failedReads.isEmpty()) {
              mismatchDetector.batch(matchingSuccessfulReads.failedReads, throwable);
            }
          }
        }
      }

      private void consumeWriteErrors(List<? extends Row> writeOperations, Object[] writeResults) {
        try (Scope scope = mirroringTracer.spanFactory.writeErrorScope()) {
          for (int i = 0; i < writeOperations.size(); i++) {
            Throwable cause =
                writeResults[i] instanceof Throwable ? (Throwable) writeResults[i] : null;

            Row operation = writeOperations.get(i);
            secondaryWriteErrorConsumer.consume(HBaseOperation.BATCH, operation, cause);
          }
        }
      }
    };
  }

  /**
   * Helper class that facilitates analysing results of partially completed batch operation
   * containing {@link Get}s. Contains matching results from first and secondary databases, Get
   * operations that produced those results, and Gets that failed on secondary.
   */
  private static class MatchingSuccessfulReadsResults {
    final Result[] primaryResults;
    final Result[] secondaryResults;
    final List<Get> failedReads;
    final List<Get> successfulReads;

    private MatchingSuccessfulReadsResults(
        Result[] primaryResults,
        Result[] secondaryResults,
        List<Get> failedReads,
        List<Get> successfulReads) {
      this.primaryResults = primaryResults;
      this.secondaryResults = secondaryResults;
      this.failedReads = failedReads;
      this.successfulReads = successfulReads;
    }
  }

  /**
   * Creates a {@link MatchingSuccessfulReadsResults} based on arrays of results from primary and
   * secondary databases and list of performed operations. All inputs are iterated simultaneously,
   * Get operations are identified using isinstance and f their results from both databases are
   * available, they are added to lists of matching reads and successful operations. In the other
   * case the Get operation is placed on failed operations list.
   */
  private static MatchingSuccessfulReadsResults selectMatchingSuccessfulReads(
      List<? extends Row> operations,
      Object[] primaryResults,
      Object[] secondaryResults,
      Predicate<Object> resultIsFaultyPredicate) {
    assert operations.size() == secondaryResults.length;
    assert primaryResults.length == secondaryResults.length;

    List<Result> primaryMatchingReads = new ArrayList<>();
    List<Result> secondaryMatchingReads = new ArrayList<>();

    List<Get> failedReads = new ArrayList<>();
    List<Get> successfulReads = new ArrayList<>();

    for (int i = 0; i < secondaryResults.length; i++) {
      if (!(operations.get(i) instanceof Get)) {
        continue;
      }

      // We are sure casts are correct, and non-failed results to Gets are always Results.
      if (resultIsFaultyPredicate.apply(secondaryResults[i])) {
        failedReads.add((Get) operations.get(i));
      } else {
        primaryMatchingReads.add((Result) primaryResults[i]);
        secondaryMatchingReads.add((Result) secondaryResults[i]);
        successfulReads.add((Get) operations.get(i));
      }
    }

    return new MatchingSuccessfulReadsResults(
        primaryMatchingReads.toArray(new Result[0]),
        secondaryMatchingReads.toArray(new Result[0]),
        failedReads,
        successfulReads);
  }

  /**
   * Helper class facilitating analysis of {@link Table#batch(List, Object[])} results. Splits
   * operations and corresponding results into failed and successful based on contents of results.
   */
  public static class FailedSuccessfulSplit<T extends Row> {
    public final List<T> successfulOperations = new ArrayList<>();
    public final Result[] successfulResults;
    public final List<T> failedOperations = new ArrayList<>();
    public final Object[] failedResults;

    public FailedSuccessfulSplit(
        List<T> operations, Object[] results, Predicate<Object> resultIsFaultyPredicate) {
      List<Result> successfulResultsList = new ArrayList<>();
      List<Object> failedResultsList = new ArrayList<>();
      for (int i = 0; i < operations.size(); i++) {
        T operation = operations.get(i);
        Object result = results[i];
        boolean isFailed = resultIsFaultyPredicate.apply(result);
        if (isFailed) {
          failedOperations.add(operation);
          failedResultsList.add(result);
        } else {
          successfulOperations.add(operation);
          successfulResultsList.add((Result) result);
        }
      }
      this.successfulResults = successfulResultsList.toArray(new Result[0]);
      this.failedResults = failedResultsList.toArray(new Object[0]);
    }
  }

  /**
   * Helper class facilitating analysis of {@link Table#batch(List, Object[])} results. Splits
   * operations and corresponding results into reads and writes based on types of operations.
   */
  public static class ReadWriteSplit<OperationType extends Row, ReadResultType> {
    public final List<Get> readOperations = new ArrayList<>();
    public final List<OperationType> writeOperations = new ArrayList<>();
    public final ReadResultType[] readResults;
    public final Object[] writeResults;

    public ReadWriteSplit(
        List<OperationType> operations,
        Object[] results,
        Class<ReadResultType> readResultTypeClass) {
      final List<ReadResultType> readResultsList = new ArrayList<>();
      final List<Object> writeResultsList = new ArrayList<>();

      for (int i = 0; i < operations.size(); i++) {
        OperationType operation = operations.get(i);
        Object result = results[i];
        boolean isRead = operation instanceof Get;
        if (isRead) {
          readOperations.add((Get) operation);
          readResultsList.add((ReadResultType) result);
        } else {
          writeOperations.add(operation);
          writeResultsList.add(result);
        }
      }

      this.readResults =
          readResultsList.toArray((ReadResultType[]) Array.newInstance(readResultTypeClass, 0));
      this.writeResults = writeResultsList.toArray(new Object[0]);
    }
  }
}
