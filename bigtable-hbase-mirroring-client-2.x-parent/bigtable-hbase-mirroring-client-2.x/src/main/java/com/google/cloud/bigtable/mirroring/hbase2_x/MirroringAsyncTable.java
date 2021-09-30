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

import static com.google.cloud.bigtable.mirroring.hbase2_x.utils.AsyncRequestScheduling.reserveFlowControlResourcesThenScheduleSecondary;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringTable;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestResourcesDescription;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.cloud.bigtable.mirroring.hbase2_x.utils.futures.FutureConverter;
import com.google.common.util.concurrent.FutureCallback;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScanResultConsumerBase;
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcChannel;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

public class MirroringAsyncTable<C extends ScanResultConsumerBase> implements AsyncTable<C> {
  private final AsyncTable<C> primaryTable;
  private final AsyncTable<C> secondaryTable;
  private final VerificationContinuationFactory verificationContinuationFactory;
  private final FlowController flowController;
  private final SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer;
  private final MirroringTracer mirroringTracer;

  public MirroringAsyncTable(
      AsyncTable<C> primaryTable,
      AsyncTable<C> secondaryTable,
      MismatchDetector mismatchDetector,
      FlowController flowController,
      SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumer,
      MirroringTracer mirroringTracer) {
    this.primaryTable = primaryTable;
    this.secondaryTable = secondaryTable;
    this.verificationContinuationFactory = new VerificationContinuationFactory(mismatchDetector);
    this.flowController = flowController;
    this.secondaryWriteErrorConsumer = secondaryWriteErrorConsumer;
    this.mirroringTracer = mirroringTracer;
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    CompletableFuture<Result> primaryFuture = this.primaryTable.get(get);
    return readWithVerificationAndFlowControl(
        RequestResourcesDescription::new,
        primaryFuture,
        () -> this.secondaryTable.get(get),
        (result) -> this.verificationContinuationFactory.get(get, result));
  }

  @Override
  public CompletableFuture<Boolean> exists(Get get) {
    CompletableFuture<Boolean> primaryFuture = this.primaryTable.exists(get);
    return readWithVerificationAndFlowControl(
        RequestResourcesDescription::new,
        primaryFuture,
        () -> this.secondaryTable.exists(get),
        (result) -> this.verificationContinuationFactory.exists(get, result));
  }

  @Override
  public CompletableFuture<Void> put(Put put) {
    CompletableFuture<Void> primaryFuture = this.primaryTable.put(put);
    return writeWithFlowControl(
        new MirroringTable.WriteOperationInfo(put),
        HBaseOperation.PUT,
        primaryFuture,
        () -> this.secondaryTable.put(put),
        () -> this.secondaryWriteErrorConsumer.consume(HBaseOperation.PUT, put));
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete) {
    CompletableFuture<Void> primaryFuture = this.primaryTable.delete(delete);
    return writeWithFlowControl(
        new MirroringTable.WriteOperationInfo(delete),
        HBaseOperation.DELETE,
        primaryFuture,
        () -> this.secondaryTable.delete(delete),
        () -> this.secondaryWriteErrorConsumer.consume(HBaseOperation.DELETE, delete));
  }

  @Override
  public CompletableFuture<Result> append(Append append) {
    CompletableFuture<Result> primaryFuture = this.primaryTable.append(append);
    return writeWithFlowControl(
        new MirroringTable.WriteOperationInfo(append),
        HBaseOperation.APPEND,
        primaryFuture,
        () -> this.secondaryTable.append(append),
        () -> this.secondaryWriteErrorConsumer.consume(HBaseOperation.APPEND, append));
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment) {
    CompletableFuture<Result> primaryFuture = this.primaryTable.increment(increment);
    return writeWithFlowControl(
        new MirroringTable.WriteOperationInfo(increment),
        HBaseOperation.INCREMENT,
        primaryFuture,
        () -> this.secondaryTable.increment(increment),
        () -> this.secondaryWriteErrorConsumer.consume(HBaseOperation.INCREMENT, increment));
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations rowMutations) {
    CompletableFuture<Void> primaryFuture = this.primaryTable.mutateRow(rowMutations);
    return writeWithFlowControl(
        new MirroringTable.WriteOperationInfo(rowMutations),
        HBaseOperation.MUTATE_ROW,
        primaryFuture,
        () -> this.secondaryTable.mutateRow(rowMutations),
        () -> this.secondaryWriteErrorConsumer.consume(HBaseOperation.MUTATE_ROW, rowMutations));
  }

  private <T> CompletableFuture<T> readWithVerificationAndFlowControl(
      final Function<T, RequestResourcesDescription> resourcesDescriptionCreator,
      final CompletableFuture<T> primaryFuture,
      final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
      final Function<T, FutureCallback<T>> verificationCallbackCreator) {
    CompletableFuture<T> resultFuture = new CompletableFuture<T>();

    primaryFuture.whenComplete(
        (primaryResult, primaryError) -> {
          if (primaryError != null) {
            resultFuture.completeExceptionally(primaryError);
            return;
          }
          reserveFlowControlResourcesThenScheduleSecondary(
                  primaryFuture,
                  FutureConverter.toCompletable(
                      flowController.asyncRequestResource(
                          resourcesDescriptionCreator.apply(primaryResult))),
                  secondaryFutureSupplier,
                  verificationCallbackCreator)
              .whenComplete(
                  (ignoredResult, ignoredError) -> {
                    resultFuture.complete(primaryResult);
                  });
        });

    return resultFuture;
  }

  private <T> CompletableFuture<T> writeWithFlowControl(
      final MirroringTable.WriteOperationInfo writeOperationInfo,
      final HBaseOperation operation,
      final CompletableFuture<T> primaryFuture,
      final Supplier<CompletableFuture<T>> secondaryFutureSupplier,
      final Runnable controlFlowReservationErrorHandler) {
    return reserveFlowControlResourcesThenScheduleSecondary(
        primaryFuture,
        FutureConverter.toCompletable(
            flowController.asyncRequestResource(writeOperationInfo.requestResourcesDescription)),
        secondaryFutureSupplier,
        (ignoredSecondaryResult) ->
            new FutureCallback<T>() {
              @Override
              public void onSuccess(@NullableDecl T t) {}

              @Override
              public void onFailure(Throwable throwable) {
                secondaryWriteErrorConsumer.consume(operation, writeOperationInfo.operations);
              }
            },
        controlFlowReservationErrorHandler);
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
  public CheckAndMutateBuilder checkAndMutate(byte[] bytes, byte[] bytes1) {
    throw new UnsupportedOperationException();
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
  public List<CompletableFuture<Result>> get(List<Get> list) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CompletableFuture<Void>> put(List<Put> list) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CompletableFuture<Void>> delete(List<Delete> list) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions) {
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
}
