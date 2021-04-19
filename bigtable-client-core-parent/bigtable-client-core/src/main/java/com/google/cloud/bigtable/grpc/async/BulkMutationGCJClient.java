/*
 * Copyright 2019 Google LLC
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
import com.google.api.gax.batching.Batcher;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.common.base.Preconditions;
import java.io.IOException;

/**
 * This class is meant to replicate existing {@link BulkMutation} while translating calls to *
 * Google-Cloud-Java's {@link Batcher} api.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BulkMutationGCJClient implements IBulkMutation {

  private final Batcher<RowMutationEntry, Void> bulkMutateBatcher;

  public BulkMutationGCJClient(Batcher<RowMutationEntry, Void> bulkMutateBatcher) {
    this.bulkMutateBatcher = bulkMutateBatcher;
  }

  /** {@inheritDoc} */
  @Override
  public synchronized ApiFuture<Void> add(RowMutationEntry rowMutation) {
    Preconditions.checkNotNull(rowMutation, "mutation details cannot be null");
    return bulkMutateBatcher.add(rowMutation);
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void sendUnsent() {
    bulkMutateBatcher.sendOutstanding();
  }

  /** {@inheritDoc} */
  @Override
  public synchronized void flush() {
    try {
      bulkMutateBatcher.flush();
    } catch (InterruptedException ex) {
      throw new RuntimeException("Could not complete RPC for current Batch", ex);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    try {
      bulkMutateBatcher.close();
    } catch (InterruptedException e) {
      throw new IOException("Could not close the bulk mutation Batcher", e);
    }
  }
}
