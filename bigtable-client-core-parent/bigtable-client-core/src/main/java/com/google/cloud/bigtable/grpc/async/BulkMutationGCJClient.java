/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.async;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher.BulkMutationFailure;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.util.ApiFutureUtil;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * This class is meant to replicate existing {@link BulkMutation} while translating calls to
 * Google-Cloud-Java's {@link BulkMutationBatcher} api.
 */
@InternalApi("For internal usage only")
public class BulkMutationGCJClient implements IBulkMutation {

  private static Logger LOG = new Logger(BulkMutationGCJClient.class);

  private final BulkMutationBatcher bulkMutateBatcher;
  private final OperationAccountant operationAccountant;

  public BulkMutationGCJClient(BulkMutationBatcher bulkMutateBatcher) {
    this.bulkMutateBatcher = bulkMutateBatcher;
    this.operationAccountant = new OperationAccountant();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ApiFuture<Void> add(RowMutation rowMutation) {
    Preconditions.checkNotNull(rowMutation, "mutation details cannot be null");
    final ApiFuture<Void> response = bulkMutateBatcher.add(rowMutation);
    operationAccountant.registerOperation(ApiFutureUtil.adapt(response));
    return response;
  }

  /** {@inheritDoc} */
  @Override
  public void sendUnsent() {
    LOG.info("This operation will be implemented once the underlying API has this feature.");
  }

  /** {@inheritDoc} */
  @Override
  public void flush() {
    try {
      operationAccountant.awaitCompletion();
    } catch (BulkMutationFailure | InterruptedException ex) {
      throw new RuntimeException("Could not complete RPC for current Batch", ex);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean isFlushed() {
    return !operationAccountant.hasInflightOperations();
  }

  @Override
  public void close() throws IOException {
    try {
      bulkMutateBatcher.close();
    } catch (InterruptedException | TimeoutException e) {
      throw new IOException("Could not close the bulk mutation Batcher", e);
    }
  }
}
