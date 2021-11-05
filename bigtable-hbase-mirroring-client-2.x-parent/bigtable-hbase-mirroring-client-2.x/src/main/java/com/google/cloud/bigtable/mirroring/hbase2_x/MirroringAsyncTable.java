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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.OperationUtils.makePutFromResult;
import static com.google.cloud.bigtable.mirroring.hbase2_x.utils.AsyncRequestScheduling.reserveFlowControlResourcesThenScheduleSecondary;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringTable;
import com.google.cloud.bigtable.mirroring.hbase1_x.WriteOperationFutureCallback;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.FailedSuccessfulSplit;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.BatchHelpers.ReadWriteSplit;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.AsyncRequestScheduling.OperationStages;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureConverter;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureUtils;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.FutureCallback;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScanResultConsumerBase;
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class MirroringAsyncTable<C extends ScanResultConsumerBase> implements AsyncTable<C> {
  private final Predicate<Object> resultIsFaultyPredicate = (o) -> o instanceof Throwable;
  private final AsyncTable<C> primaryTable;
  private final AsyncTable<C> secondaryTable;
  private final VerificationContinuationFactory verificationContinuationFactory;
  private final FlowController flowController;
  private final SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  private final MirroringTracer mirroringTracer;
  private final ListenableReferenceCounter referenceCounter;

  public MirroringAsyncTable(
      AsyncTable<C> primaryTable,
      AsyncTable<C> secondaryTable,
      MismatchDetector mismatchDetector,
      FlowController flowController,
      SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer,
      MirroringTracer mirroringTracer,
      ListenableReferenceCounter referenceCounter) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
    this.verificationContinuationFactory = new VerificationContinuationFactory(mismatchDetector);
    this.flowController = flowController;
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
    this.mirroringTracer = mirroringTracer;
    this.referenceCounter = referenceCounter;
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    CompletableFuture<Result> primaryFuture = this.primaryTable.get(get);
    return readWithVerificationAndFlowControl(
            RequestResourcesDescription::new,
            primaryFuture,
            () -> this.secondaryTable.get(get),
            (result) -> this.verificationContinuationFactory.get(get, result))
        .userNotified;
  }

  @Override
  public CompletableFuture<Boolean> exists(Get get) {
    CompletableFuture<Boolean> primaryFuture = this.primaryTable.exists(get);
    return readWithVerificationAndFlowControl(
            RequestResourcesDescription::new,
            primaryFuture,
            () -> this.secondaryTable.exists(get),
            (result) -> this.verificationContinuationFactory.exists(get, result))
        .userNotified;
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    CompletableFuture<Void> primaryFuture = this.primaryTable.put(put);
    return writeWithFlowControl(
            new MirroringTable.WriteOperationInfo(put),
            primaryFuture,
            () -> this.secondaryTable.put(put))
        .userNotified;
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    CompletableFuture<Void> primaryFuture = this.primaryTable.delete(delete);
    return writeWithFlowControl(
            new MirroringTable.WriteOperationInfo(delete),
            primaryFuture,
            () -> this.secondaryTable.delete(delete))
        .userNotified;
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    CompletableFuture<Result> primaryFuture = this.primaryTable.append(append);
    return mutationAsPut(primaryFuture).userNotified;
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    CompletableFuture<Result> primaryFuture = this.primaryTable.increment(increment);
    return mutationAsPut(primaryFuture).userNotified;
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations rowMutations) {
    CompletableFuture<Void> primaryFuture = this.primaryTable.mutateRow(rowMutations);
    return writeWithFlowControl(
            new MirroringTable.WriteOperationInfo(rowMutations),
            primaryFuture,
            () -> this.secondaryTable.mutateRow(rowMutations))
        .userNotified;
  }

  @Override
  public List<CompletableFuture<Result>> get(List<Get> list) {
    return generalBatch(
            list,
            this.primaryTable::get,
            this.secondaryTable::get,
            BatchBuilder<Get, Result>::new,
            Result.class)
        .userNotified;
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> list) {
    return generalBatch(
            list,
            this.primaryTable::put,
            this.secondaryTable::put,
            BatchBuilder<Put, Void>::new,
            Void.class)
        .userNotified;
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> list) {
    return generalBatch(
            list,
            this.primaryTable::delete,
            this.secondaryTable::delete,
            BatchBuilder<Delete, Void>::new,
            Void.class)
        .userNotified;
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
    return this.<T, Row, Object>generalBatch(
            actions,
            this.primaryTable::batch,
            this.secondaryTable::batch,
            BatchBuilder<Row, Object>::new,
            Object.class)
        .userNotified;
  }

  @Override
  public List<CompletableFuture<Boolean>> exists(List<Get> list) {
    return generalBatch(
            list,
            this.primaryTable::exists,
            this.secondaryTable::exists,
            ExistsBuilder::new,
            Boolean.class)
        .userNotified;
  }

  public <ResultType, ActionType extends Row, SuccessfulResultType>
      OperationStages<List<CompletableFuture<ResultType>>> generalBatch(
          List<? extends ActionType> userActions,
          Function<List<ActionType>, List<CompletableFuture<ResultType>>> primaryFunction,
          Function<List<ActionType>, List<CompletableFuture<ResultType>>> secondaryFunction,
          Function<FailedSuccessfulSplit<ActionType, SuccessfulResultType>, GeneralBatchBuilder>
              batchBuilderCreator,
          Class<SuccessfulResultType> successfulResultTypeClass) {
    List<ActionType> actions = new ArrayList<>(userActions);
    final int numActions = actions.size();

    final OperationStages<List<CompletableFuture<ResultType>>> returnedValue =
        new OperationStages<>(generateList(numActions, CompletableFuture<ResultType>::new));

    final List<CompletableFuture<ResultType>> primaryFutures = primaryFunction.apply(actions);
    final Object[] primaryResults = new Object[numActions];

    BiConsumer<Integer, Throwable> primaryErrorHandler =
        (idx, throwable) -> returnedValue.userNotified.get(idx).completeExceptionally(throwable);
    waitForAllWithErrorHandler(primaryFutures, primaryErrorHandler, primaryResults)
        .whenComplete(
            (ignoredResult, ignoredError) -> {
              final FailedSuccessfulSplit<ActionType, SuccessfulResultType> failedSuccessfulSplit =
                  new FailedSuccessfulSplit<>(
                      actions, primaryResults, resultIsFaultyPredicate, successfulResultTypeClass);

              if (failedSuccessfulSplit.successfulOperations.isEmpty()) {
                // All results were instances of Throwable, so we already completed
                // exceptionally result futures with errorHandler passed to
                // waitForAllWithErrorHandler.
                returnedValue.verificationCompleted();
                return;
              }

              GeneralBatchBuilder batchBuilder = batchBuilderCreator.apply(failedSuccessfulSplit);

              final List<ActionType> operationsToScheduleOnSecondary =
                  failedSuccessfulSplit.successfulOperations;

              final Object[] secondaryResults = new Object[operationsToScheduleOnSecondary.size()];

              ReadWriteSplit<ActionType, SuccessfulResultType> successfulReadWriteSplit =
                  new ReadWriteSplit<>(
                      failedSuccessfulSplit.successfulOperations,
                      failedSuccessfulSplit.successfulResults,
                      successfulResultTypeClass);

              final RequestResourcesDescription requestResourcesDescription =
                  batchBuilder.getRequestResourcesDescription(operationsToScheduleOnSecondary);

              final CompletableFuture<FlowController.ResourceReservation>
                  resourceReservationRequest =
                      FutureConverter.toCompletable(
                          this.flowController.asyncRequestResource(requestResourcesDescription));

              resourceReservationRequest.whenComplete(
                  (ignoredResourceReservation, resourceReservationError) -> {
                    completeSuccessfulResultFutures(returnedValue.userNotified, primaryResults);
                    if (resourceReservationError != null) {
                      if (!successfulReadWriteSplit.writeOperations.isEmpty()) {
                        this.secondaryWriteErrorConsumer.consume(
                            HBaseOperation.BATCH,
                            successfulReadWriteSplit.writeOperations,
                            resourceReservationError);
                      }
                      return;
                    }
                    FutureUtils.forwardResult(
                        reserveFlowControlResourcesThenScheduleSecondary(
                                CompletableFuture.completedFuture(null),
                                resourceReservationRequest,
                                () ->
                                    waitForAllWithErrorHandler(
                                        secondaryFunction.apply(operationsToScheduleOnSecondary),
                                        (idx, throwable) -> {},
                                        secondaryResults),
                                (ignoredPrimaryResult) ->
                                    batchBuilder.getVerificationCallback(secondaryResults))
                            .getVerificationCompletedFuture(),
                        returnedValue.getVerificationCompletedFuture());
                  });
            });
    return wrapWithReferenceCounter(returnedValue);
  }

  private <T> ArrayList<T> generateList(int size, Supplier<T> initializer) {
    return Stream.generate(initializer)
        .limit(size)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  private <T> void completeSuccessfulResultFutures(
      List<CompletableFuture<T>> resultFutures, Object[] primaryResults) {
    for (int i = 0; i < primaryResults.length; i++) {
      if (!(resultIsFaultyPredicate.apply(primaryResults[i]))) {
        resultFutures.get(i).complete((T) primaryResults[i]);
      }
    }
  }

  private <T> CompletableFuture<Void> waitForAllWithErrorHandler(
      List<CompletableFuture<T>> futures,
      BiConsumer<Integer, Throwable> errorHandler,
      Object[] results) {
    int numFutures = futures.size();
    List<CompletableFuture<Void>> handledFutures = new ArrayList<>(numFutures);
    for (int i = 0; i < numFutures; i++) {
      final int futureIdx = i;
      handledFutures.add(
          futures
              .get(futureIdx)
              .handle(
                  (result, error) -> {
                    if (error != null) {
                      results[futureIdx] = error;
                      errorHandler.accept(futureIdx, error);
                      throw new CompletionException(error);
                    }
                    results[futureIdx] = result;
                    return null;
                  }));
    }
    return CompletableFuture.allOf(handledFutures.toArray(new CompletableFuture[0]));
  }

  private <T extends Mutation> OperationStages<CompletableFuture<Result>> mutationAsPut(
      CompletableFuture<Result> primaryFuture) {
    OperationStages<CompletableFuture<Result>> returnedValue =
        new OperationStages<>(new CompletableFuture<>());
    primaryFuture
        .thenAccept(
            (primaryResult) -> {
              Put put = makePutFromResult(primaryResult);
              FutureUtils.forwardResult(
                  writeWithFlowControl(
                      new MirroringTable.WriteOperationInfo(put),
                      CompletableFuture.completedFuture(primaryResult),
                      () -> this.secondaryTable.put(put).thenApply(ignored -> null)),
                  returnedValue);
            })
        .exceptionally(
            primaryError -> {
              returnedValue.userNotified.completeExceptionally(primaryError);
              returnedValue.verificationCompleted();
              throw new CompletionException(primaryError);
            });
    return wrapWithReferenceCounter(returnedValue);
  }

  private <T> OperationStages<CompletableFuture<T>> readWithVerificationAndFlowControl(
      final Function<T, RequestResourcesDescription> resourcesDescriptionCreator,
      final CompletableFuture<T> primaryFuture,
      final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
      final Function<T, FutureCallback<T>> verificationCallbackCreator) {
    OperationStages<CompletableFuture<T>> returnedValue =
        new OperationStages<>(new CompletableFuture<>());
    primaryFuture.whenComplete(
        (primaryResult, primaryError) -> {
          if (primaryError != null) {
            returnedValue.userNotified.completeExceptionally(primaryError);
            returnedValue.verificationCompleted();
            return;
          }
          FutureUtils.forwardResult(
              reserveFlowControlResourcesThenScheduleSecondary(
                  primaryFuture,
                  FutureConverter.toCompletable(
                      flowController.asyncRequestResource(
                          resourcesDescriptionCreator.apply(primaryResult))),
                  secondaryFutureSupplier,
                  verificationCallbackCreator),
              returnedValue);
        });
    return wrapWithReferenceCounter(returnedValue);
  }

  private <T> OperationStages<CompletableFuture<T>> writeWithFlowControl(
      final MirroringTable.WriteOperationInfo writeOperationInfo,
      final CompletableFuture<T> primaryFuture,
      final Supplier<CompletableFuture<T>> secondaryFutureSupplier) {
    final Consumer<Throwable> secondaryWriteErrorHandler =
        (error) ->
            this.secondaryWriteErrorConsumer.consume(
                writeOperationInfo.hBaseOperation, writeOperationInfo.operations, error);
    return wrapWithReferenceCounter(
        reserveFlowControlResourcesThenScheduleSecondary(
            primaryFuture,
            FutureConverter.toCompletable(
                flowController.asyncRequestResource(
                    writeOperationInfo.requestResourcesDescription)),
            secondaryFutureSupplier,
            (ignoredSecondaryResult) ->
                new WriteOperationFutureCallback<T>() {
                  @Override
                  public void onFailure(Throwable throwable) {
                    secondaryWriteErrorHandler.accept(throwable);
                  }
                },
            secondaryWriteErrorHandler));
  }

  private <T> OperationStages<T> wrapWithReferenceCounter(OperationStages<T> toBeReferenceCounted) {
    keepReferenceUntilOperationCompletes(toBeReferenceCounted.getVerificationCompletedFuture());
    return toBeReferenceCounted;
  }

  private void keepReferenceUntilOperationCompletes(CompletableFuture<Void> future) {
    this.referenceCounter.incrementReferenceCount();
    future.whenComplete(
        (ignoredResult, ignoredError) -> this.referenceCounter.decrementReferenceCount());
  }

  @Override
  public TableName getName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getRpcTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getReadRpcTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getWriteRpcTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOperationTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getScanTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CheckAndMutateBuilder checkAndMutate(byte[] row, byte[] family) {
    return new MirroringCheckAndMutateBuilder(this.primaryTable.checkAndMutate(row, family));
  }

  @Override
  public void scan(Scan scan, C c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultScanner getScanner(Scan scan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<List<Result>> scanAll(Scan scan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <S, R> CompletableFuture<R> coprocessorService(
      Function<RpcChannel, S> function, ServiceCaller<S, R> serviceCaller, byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <S, R> CoprocessorServiceBuilder<S, R> coprocessorService(
      Function<RpcChannel, S> function,
      ServiceCaller<S, R> serviceCaller,
      CoprocessorCallback<R> coprocessorCallback) {
    throw new UnsupportedOperationException();
  }

  private class MirroringCheckAndMutateBuilder implements CheckAndMutateBuilder {
    private final CheckAndMutateBuilder primaryBuilder;

    public MirroringCheckAndMutateBuilder(CheckAndMutateBuilder primaryBuilder) {
      this.primaryBuilder = primaryBuilder;
    }

    private OperationStages<CompletableFuture<Boolean>> checkAndMutate(
        MirroringTable.WriteOperationInfo writeOperationInfo,
        CompletableFuture<Boolean> primary,
        Supplier<CompletableFuture<Void>> secondary) {
      OperationStages<CompletableFuture<Boolean>> returnedValue =
          new OperationStages<>(new CompletableFuture<>());
      primary
          .thenAccept(
              (wereMutationsApplied) -> {
                if (wereMutationsApplied) {
                  FutureUtils.forwardResult(
                      writeWithFlowControl(
                          writeOperationInfo,
                          CompletableFuture.completedFuture(wereMutationsApplied),
                          () -> secondary.get().thenApply(ignored -> null)),
                      returnedValue);
                } else {
                  returnedValue.userNotified.complete(wereMutationsApplied);
                  returnedValue.verificationCompleted();
                }
              })
          .exceptionally(
              primaryError -> {
                returnedValue.userNotified.completeExceptionally(primaryError);
                returnedValue.verificationCompleted();
                throw new CompletionException(primaryError);
              });
      return wrapWithReferenceCounter(returnedValue);
    }

    @Override
    public CompletableFuture<Boolean> thenPut(Put put) {
      return checkAndMutate(
              new MirroringTable.WriteOperationInfo(put),
              this.primaryBuilder.thenPut(put),
              () -> secondaryTable.put(put))
          .userNotified;
    }

    @Override
    public CompletableFuture<Boolean> thenDelete(Delete delete) {
      return checkAndMutate(
              new MirroringTable.WriteOperationInfo(delete),
              this.primaryBuilder.thenDelete(delete),
              () -> secondaryTable.delete(delete))
          .userNotified;
    }

    @Override
    public CompletableFuture<Boolean> thenMutate(RowMutations rowMutations) {
      return checkAndMutate(
              new MirroringTable.WriteOperationInfo(rowMutations),
              this.primaryBuilder.thenMutate(rowMutations),
              () -> secondaryTable.mutateRow(rowMutations))
          .userNotified;
    }

    @Override
    public CheckAndMutateBuilder qualifier(byte[] bytes) {
      this.primaryBuilder.qualifier(bytes);
      return this;
    }

    @Override
    public CheckAndMutateBuilder timeRange(TimeRange timeRange) {
      this.primaryBuilder.timeRange(timeRange);
      return this;
    }

    @Override
    public CheckAndMutateBuilder ifNotExists() {
      this.primaryBuilder.ifNotExists();
      return this;
    }

    @Override
    public CheckAndMutateBuilder ifMatches(CompareOperator compareOperator, byte[] bytes) {
      this.primaryBuilder.ifMatches(compareOperator, bytes);
      return this;
    }
  }

  private interface GeneralBatchBuilder {
    RequestResourcesDescription getRequestResourcesDescription(
        List<? extends Row> operationsToPerformOnSecondary);

    FutureCallback<Void> getVerificationCallback(Object[] secondaryResults);
  }

  private class BatchBuilder<ActionType extends Row, SuccessfulResultType>
      implements GeneralBatchBuilder {
    final FailedSuccessfulSplit<ActionType, SuccessfulResultType> failedSuccessfulSplit;
    final ReadWriteSplit<ActionType, Result> successfulReadWriteSplit;

    BatchBuilder(FailedSuccessfulSplit<ActionType, SuccessfulResultType> split) {
      this.failedSuccessfulSplit = split;
      this.successfulReadWriteSplit =
          new ReadWriteSplit<>(
              failedSuccessfulSplit.successfulOperations,
              failedSuccessfulSplit.successfulResults,
              Result.class);
    }

    @Override
    public RequestResourcesDescription getRequestResourcesDescription(
        List<? extends Row> operationsToPerformOnSecondary) {
      return new RequestResourcesDescription(
          operationsToPerformOnSecondary, successfulReadWriteSplit.readResults);
    }

    @Override
    public FutureCallback<Void> getVerificationCallback(Object[] secondaryResults) {
      return BatchHelpers.createBatchVerificationCallback(
          this.failedSuccessfulSplit,
          this.successfulReadWriteSplit,
          secondaryResults,
          verificationContinuationFactory.getMismatchDetector(),
          secondaryWriteErrorConsumer,
          resultIsFaultyPredicate,
          mirroringTracer);
    }
  }

  private class ExistsBuilder implements GeneralBatchBuilder {
    final FailedSuccessfulSplit<Get, Boolean> primaryFailedSuccessfulSplit;
    final boolean[] primarySuccessfulResults;

    ExistsBuilder(FailedSuccessfulSplit<Get, Boolean> split) {
      this.primaryFailedSuccessfulSplit = split;
      this.primarySuccessfulResults =
          new boolean[this.primaryFailedSuccessfulSplit.successfulResults.length];
      for (int i = 0; i < this.primaryFailedSuccessfulSplit.successfulResults.length; i++) {
        this.primarySuccessfulResults[i] = this.primaryFailedSuccessfulSplit.successfulResults[i];
      }
    }

    @Override
    public RequestResourcesDescription getRequestResourcesDescription(
        List<? extends Row> operationsToPerformOnSecondary) {
      return new RequestResourcesDescription(primarySuccessfulResults);
    }

    @Override
    public FutureCallback<Void> getVerificationCallback(Object[] secondaryResults) {
      return new FutureCallback<Void>() {
        @Override
        public void onSuccess(@NullableDecl Void unused) {
          boolean[] booleanSecondaryResults = new boolean[secondaryResults.length];
          for (int i = 0; i < secondaryResults.length; i++) {
            booleanSecondaryResults[i] = (boolean) secondaryResults[i];
          }

          verificationContinuationFactory
              .getMismatchDetector()
              .existsAll(
                  primaryFailedSuccessfulSplit.successfulOperations,
                  primarySuccessfulResults,
                  booleanSecondaryResults);
        }

        @Override
        public void onFailure(Throwable error) {
          verificationContinuationFactory
              .getMismatchDetector()
              .existsAll(primaryFailedSuccessfulSplit.successfulOperations, error);
        }
      };
    }
  }
}
