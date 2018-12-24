/*
 * Copyright 2018 Google LLC.  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc;

import com.google.api.core.ApiFuture;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;

/**
 * This class implements the {@link IBigtableDataClient} interface which provides access to google cloud
 * java
 */
public class BigtableDataClientWrapper implements IBigtableDataClient {

  private final BigtableDataClient delegate;
  private final RequestContext requestContext;

  public BigtableDataClientWrapper(BigtableDataClient bigtableDataClient,
      BigtableOptions options) {
    this.delegate = bigtableDataClient;
    this.requestContext = RequestContext
        .create(InstanceName.of(options.getProjectId(),
            options.getInstanceId()),
            options.getAppProfileId()
        );
  }

  @Override
  public void mutateRow(RowMutation rowMutation) {
    MutateRowRequest mutateRowRequest = rowMutation.toProto(requestContext);
    delegate.mutateRow(mutateRowRequest);
  }

  @Override
  public ListenableFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    ListenableFuture<MutateRowResponse> response =
        delegate.mutateRowAsync(rowMutation.toProto(requestContext));
    return  Futures.transform(response, new Function<MutateRowResponse, Void>() {

      @NullableDecl
      @Override
      public Void apply(@NullableDecl MutateRowResponse input) {
        return null;
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public IBulkMutation createBulkMutationBatcher() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ListenableFuture<Boolean> checkAndMutateRowAsync(
      ConditionalRowMutation conditionalRowMutation) {
    final CheckAndMutateRowRequest request = conditionalRowMutation.toProto(requestContext);
    final ListenableFuture<CheckAndMutateRowResponse> response =
        delegate.checkAndMutateRowAsync(request);
    return Futures.transform(response, new Function<CheckAndMutateRowResponse, Boolean>() {
      @NullableDecl
      @Override
      public Boolean apply(@NullableDecl CheckAndMutateRowResponse input) {
        return wasMutationApplied(request, input);
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation) {
    CheckAndMutateRowRequest request = conditionalRowMutation.toProto(requestContext);
    CheckAndMutateRowResponse response =
        delegate.checkAndMutateRow(conditionalRowMutation.toProto(requestContext));
    return wasMutationApplied(request, response);
  }

  /**
   * This method is referred from CheckAndMutateUtil#wasMutationApplied
   */
  private static boolean wasMutationApplied(CheckAndMutateRowRequest request,
      CheckAndMutateRowResponse response) {

    // If we have true mods, we want the predicate to have matched.
    // If we have false mods, we did not want the predicate to have matched.
    return (request.getTrueMutationsCount() > 0
        && response.getPredicateMatched()) || (
        request.getFalseMutationsCount() > 0
            && !response.getPredicateMatched());
  }
}
