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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class BatchHelpers {
  public static FutureCallback<Void> createBatchVerificationCallback(
      final SplitBatchResponse<?> primarySplitResponse,
      final Object[] secondaryResults,
      final MismatchDetector mismatchDetector,
      final SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer,
      final Predicate<Object> resultIsFaultyPredicate,
      final MirroringTracer mirroringTracer) {
    return new FutureCallback<Void>() {
      @Override
      public void onSuccess(@NullableDecl Void t) {
        // Batch is successful - all results are correct.
        List<? extends Row> secondaryOperations = primarySplitResponse.allSuccessfulOperations;

        final SplitBatchResponse<?> secondarySplitResponse =
            new SplitBatchResponse<>(
                secondaryOperations, secondaryResults, resultIsFaultyPredicate);

        if (secondarySplitResponse.successfulReads.size() > 0) {
          try (Scope scope = mirroringTracer.spanFactory.verificationScope()) {
            mismatchDetector.batch(
                secondarySplitResponse.successfulReads,
                primarySplitResponse.successfulReadsResults,
                secondarySplitResponse.successfulReadsResults);
          }
        }
      }

      @Override
      public void onFailure(Throwable throwable) {
        // Batch has thrown - partial results might be available.
        List<? extends Row> secondaryOperations = primarySplitResponse.allSuccessfulOperations;

        final SplitBatchResponse<?> secondarySplitResponse =
            new SplitBatchResponse<>(
                secondaryOperations, secondaryResults, resultIsFaultyPredicate);

        if (secondarySplitResponse.failedWrites.size() > 0) {
          try (Scope scope = mirroringTracer.spanFactory.writeErrorScope()) {
            secondaryWriteErrorConsumer.consume(
                HBaseOperation.BATCH, secondarySplitResponse.failedWrites);
          }
        }

        if (secondarySplitResponse.allReads.size() > 0) {
          // Some of the reads in this batch might have been not successful.
          // We want to verify successful reads and report the others.

          // We are using `secondaryResults` to select indices of operations that were successful.
          // Using those indices we select Get operations that have results from both primary and
          // secondary database, and pass them to `mismatchDetector.batch()`.
          // We also gather failed gets to pass them to `batchGetFailure`.
          MatchingSuccessfulReadsResults matchingSuccessfulReads =
              selectMatchingSuccessfulReads(
                  secondaryOperations,
                  primarySplitResponse.allSuccessfulResults,
                  secondaryResults,
                  resultIsFaultyPredicate);

          try (Scope scope = mirroringTracer.spanFactory.verificationScope()) {
            mismatchDetector.batch(
                secondarySplitResponse.successfulReads,
                matchingSuccessfulReads.primaryResults,
                matchingSuccessfulReads.secondaryResults);

            if (!matchingSuccessfulReads.failedReads.isEmpty()) {
              mismatchDetector.batch(matchingSuccessfulReads.failedReads, throwable);
            }
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
   * Helper class facilitating analysis of batch results. Basing on issued operations and results
   * array splits provided operations into reads/writes, failed/successful.
   */
  public static class SplitBatchResponse<T extends Row> {
    public final List<Get> successfulReads = new ArrayList<>();
    public final List<T> failedWrites = new ArrayList<>();
    public final List<T> successfulWrites = new ArrayList<>();
    public final List<T> allSuccessfulOperations = new ArrayList<>();
    public final Result[] successfulReadsResults;
    public final List<Get> allReads = new ArrayList<>();
    public final Result[] allReadsResults;
    public final Object[] allSuccessfulResults;

    public SplitBatchResponse(
        List<T> operations, Object[] results, Predicate<Object> resultIsFaultyPredicate) {
      final List<Result> successfulReadsResults = new ArrayList<>();
      final List<Result> allReadsResults = new ArrayList<>();
      final List<Object> allSuccessfulResultsList = new ArrayList<>();

      for (int i = 0; i < operations.size(); i++) {
        T operation = operations.get(i);
        boolean isRead = operation instanceof Get;
        boolean isFailed = resultIsFaultyPredicate.apply(results[i]);
        if (isFailed) {
          if (isRead) {
            this.allReads.add((Get) operation);
            allReadsResults.add(null);
          } else {
            this.failedWrites.add(operation);
          }
        } else {
          if (isRead) {
            this.successfulReads.add((Get) operation);
            successfulReadsResults.add((Result) results[i]);

            this.allReads.add((Get) operation);
            allReadsResults.add((Result) results[i]);
          } else {
            this.successfulWrites.add(operation);
          }
          this.allSuccessfulOperations.add(operation);
          allSuccessfulResultsList.add(results[i]);
        }
      }
      this.successfulReadsResults = successfulReadsResults.toArray(new Result[0]);
      this.allReadsResults = allReadsResults.toArray(new Result[0]);
      this.allSuccessfulResults = allSuccessfulResultsList.toArray(new Object[0]);
    }
  }
}
