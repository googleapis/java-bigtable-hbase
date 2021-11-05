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

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringOperationException.ExceptionDetails;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.FutureCallback;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class BatchHelpers {
  public static FutureCallback<Void> createBatchVerificationCallback(
      final FailedSuccessfulSplit<?, ?> failedAndSuccessfulPrimaryOperations,
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

        final FailedSuccessfulSplit<?, Object> secondaryFailedAndSuccessfulOperations =
            new FailedSuccessfulSplit<>(
                secondaryOperations, secondaryResults, resultIsFaultyPredicate, Object.class);

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

        final FailedSuccessfulSplit<?, Object> secondaryFailedAndSuccessfulOperations =
            new FailedSuccessfulSplit<>(
                secondaryOperations, secondaryResults, resultIsFaultyPredicate, Object.class);

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
  public static class FailedSuccessfulSplit<OperationType extends Row, SuccessfulResultType> {
    public final List<OperationType> successfulOperations = new ArrayList<>();
    public final SuccessfulResultType[] successfulResults;
    public final List<OperationType> failedOperations = new ArrayList<>();
    public final Object[] failedResults;

    public FailedSuccessfulSplit(
        List<OperationType> operations,
        Object[] results,
        Predicate<Object> resultIsFaultyPredicate,
        Class<SuccessfulResultType> successfulResultTypeClass) {
      List<SuccessfulResultType> successfulResultsList = new ArrayList<>();
      List<Object> failedResultsList = new ArrayList<>();
      for (int i = 0; i < operations.size(); i++) {
        OperationType operation = operations.get(i);
        Object result = results[i];
        boolean isFailed = resultIsFaultyPredicate.apply(result);
        if (isFailed) {
          failedOperations.add(operation);
          failedResultsList.add(result);
        } else {
          successfulOperations.add(operation);
          successfulResultsList.add((SuccessfulResultType) result);
        }
      }
      this.successfulResults =
          successfulResultsList.toArray(
              (SuccessfulResultType[]) Array.newInstance(successfulResultTypeClass, 0));
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

  private static ExceptionDetails getExceptionDetails(Map<Row, ExceptionDetails> map, Row key) {
    ExceptionDetails value = map.get(key);
    if (value == null) {
      return new ExceptionDetails(new IOException("no details"));
    }
    return value;
  }

  public static void reconcileBatchResultsConcurrent(
      Object[] outputResults,
      BatchData primaryBatchData,
      BatchData secondaryBatchData,
      Predicate<Object> resultIsFaultyPredicate)
      throws RetriesExhaustedWithDetailsException {
    List<Row> failedRows = new ArrayList<>();
    List<Throwable> failureCauses = new ArrayList<>();
    List<String> hostnameAndPorts = new ArrayList<>();

    Map<Row, ExceptionDetails> failedPrimaryOperations = makeMapOfFailedRows(primaryBatchData);
    Map<Row, ExceptionDetails> failedSecondaryOperations = makeMapOfFailedRows(secondaryBatchData);

    if (failedPrimaryOperations.isEmpty() && failedSecondaryOperations.isEmpty()) {
      // No errors, return early to skip unnecessary computation.
      // This is the common case.
      return;
    }

    assert primaryBatchData.operations.size() == secondaryBatchData.operations.size();
    for (int index = 0; index < primaryBatchData.operations.size(); index++) {
      Object primaryResult = primaryBatchData.results[index];
      Object secondaryResult = secondaryBatchData.results[index];
      boolean primaryOperationFailed = resultIsFaultyPredicate.apply(primaryResult);
      boolean secondaryOperationFailed = resultIsFaultyPredicate.apply(secondaryResult);
      if (!primaryOperationFailed && !secondaryOperationFailed) {
        continue;
      }
      Row primaryOperation = primaryBatchData.operations.get(index);
      Row secondaryOperation = secondaryBatchData.operations.get(index);
      ExceptionDetails primaryExceptionDetails =
          getExceptionDetails(failedPrimaryOperations, primaryOperation);
      ExceptionDetails secondaryExceptionDetails =
          getExceptionDetails(failedSecondaryOperations, secondaryOperation);

      Throwable exception;
      String hostnameAndPort;
      if (primaryOperationFailed && secondaryOperationFailed) {
        exception =
            MirroringOperationException.markedAsBothException(
                primaryExceptionDetails.exception, secondaryExceptionDetails, secondaryOperation);
        hostnameAndPort = primaryExceptionDetails.hostnameAndPort;
      } else if (primaryOperationFailed) {
        exception =
            MirroringOperationException.markedAsPrimaryException(
                primaryExceptionDetails.exception, primaryOperation);
        hostnameAndPort = primaryExceptionDetails.hostnameAndPort;
      } else { // secondaryOperationFailed
        exception =
            MirroringOperationException.markedAsSecondaryException(
                secondaryExceptionDetails.exception, secondaryOperation);
        hostnameAndPort = secondaryExceptionDetails.hostnameAndPort;
      }
      outputResults[index] = exception;
      failureCauses.add(exception);
      failedRows.add(primaryOperation);
      hostnameAndPorts.add(hostnameAndPort);
    }
    if (!failedRows.isEmpty()) {
      throw new RetriesExhaustedWithDetailsException(failureCauses, failedRows, hostnameAndPorts);
    }
  }

  public static void reconcileBatchResultsSequential(
      Object[] outputResults,
      BatchData primaryBatchData,
      BatchData secondaryBatchData,
      Predicate<Object> resultIsFaultyPredicate)
      throws RetriesExhaustedWithDetailsException {
    List<Row> failedRows = new ArrayList<>();
    List<Throwable> failureCauses = new ArrayList<>();
    List<String> hostnameAndPorts = new ArrayList<>();

    Map<Row, ExceptionDetails> failedPrimaryOperations = makeMapOfFailedRows(primaryBatchData);
    Map<Row, ExceptionDetails> failedSecondaryOperations = makeMapOfFailedRows(secondaryBatchData);

    if (failedPrimaryOperations.isEmpty() && failedSecondaryOperations.isEmpty()) {
      // No errors, return early to skip unnecessary computation.
      // This is the common case.
      return;
    }

    assert primaryBatchData.operations.size() >= secondaryBatchData.operations.size();

    // sizes are not equal, one or more of the following is possible
    // - primary has reads that were excluded from secondary,
    // - there were operations that failed on primary and were excluded from secondary.
    // We match results from primary with corresponding result from secondary.
    int primaryIndex = 0;
    int secondaryIndex = 0;

    while (primaryIndex < primaryBatchData.operations.size()) {
      boolean primaryOperationFailed =
          resultIsFaultyPredicate.apply(primaryBatchData.results[primaryIndex]);

      // failed operations are always excluded from secondary.
      if (primaryOperationFailed) {
        Row operation = primaryBatchData.operations.get(primaryIndex);
        failedRows.add(operation);

        ExceptionDetails exceptionDetails = getExceptionDetails(failedPrimaryOperations, operation);

        Throwable exception =
            MirroringOperationException.markedAsPrimaryException(
                exceptionDetails.exception, operation);
        failureCauses.add(exception);
        outputResults[primaryIndex] = exception;
        hostnameAndPorts.add(exceptionDetails.hostnameAndPort);
        primaryIndex++;
        continue;
      }
      // Primary operation was successful, it might have been excluded from secondary if it was a
      // read. We assume that either all successful reads are excluded or none of them.
      boolean primaryIsRead = primaryBatchData.operations.get(primaryIndex) instanceof Get;
      boolean secondaryIsRead = secondaryBatchData.operations.get(secondaryIndex) instanceof Get;
      if (primaryIsRead && !secondaryIsRead) {
        // read was excluded
        primaryIndex++;
        continue;
      }

      // Otherwise a successful write was excluded, which is not possible.
      assert primaryIsRead == secondaryIsRead;

      boolean secondaryOperationFailed =
          resultIsFaultyPredicate.apply(secondaryBatchData.results[secondaryIndex]);
      if (secondaryOperationFailed) {
        Row primaryOperation = primaryBatchData.operations.get(primaryIndex);
        Row secondaryOperation = secondaryBatchData.operations.get(secondaryIndex);
        failedRows.add(primaryOperation);
        ExceptionDetails exceptionDetails =
            getExceptionDetails(failedSecondaryOperations, secondaryOperation);
        Throwable exception =
            MirroringOperationException.markedAsSecondaryException(
                exceptionDetails.exception, secondaryOperation);
        failureCauses.add(exception);
        outputResults[primaryIndex] = exception;
        hostnameAndPorts.add(exceptionDetails.hostnameAndPort);
      }
      primaryIndex++;
      secondaryIndex++;
    }
    if (!failedRows.isEmpty()) {
      throw new RetriesExhaustedWithDetailsException(failureCauses, failedRows, hostnameAndPorts);
    }
  }

  public static Map<Row, ExceptionDetails> makeMapOfFailedRows(BatchData primaryBatchData) {
    IdentityHashMap<Row, ExceptionDetails> result = new IdentityHashMap<>();

    if (primaryBatchData.exception == null) {
      return result;
    }

    if (primaryBatchData.exception instanceof RetriesExhaustedWithDetailsException) {
      RetriesExhaustedWithDetailsException exception =
          (RetriesExhaustedWithDetailsException) primaryBatchData.exception;
      for (int i = 0; i < exception.getNumExceptions(); i++) {
        result.put(
            exception.getRow(i),
            new ExceptionDetails(exception.getCause(i), exception.getHostnamePort(i)));
      }
    } else {
      for (Row r : primaryBatchData.operations) {
        result.put(r, new ExceptionDetails(primaryBatchData.exception));
      }
    }
    return result;
  }

  public static class BatchData {
    private final List<? extends Row> operations;
    private final Object[] results;
    private Throwable exception;

    public BatchData(List<? extends Row> operations, Object[] results) {
      this.operations = operations;
      this.results = results;
    }

    public List<? extends Row> getOperations() {
      return operations;
    }

    public Object[] getResults() {
      return results;
    }

    public Throwable getException() {
      return exception;
    }

    public void setException(Throwable t) {
      this.exception = t;
    }
  }

  public static boolean canBatchBePerformedConcurrently(List<? extends Row> operations) {
    // Only Puts and Deletes can be performed concurrently.
    // We assume that RowMutations can consist of only Puts and Deletes (which is true in HBase 1.x
    // and 2.x).
    for (Row operation : operations) {
      if (!(operation instanceof Put)
          && !(operation instanceof Delete)
          && !(operation instanceof RowMutations)) {
        return false;
      }
    }
    return true;
  }
}
