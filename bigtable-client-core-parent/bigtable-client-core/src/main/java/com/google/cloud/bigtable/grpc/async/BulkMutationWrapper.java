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
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.util.ApiFutureUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.io.IOException;

/**
 * This class wraps existing {@link BulkMutation} with Google-cloud-java's model.
 *
 * <p>For internal use only - public for technical reasons.
 *
 * <p>See {@link
 * com.google.cloud.bigtable.grpc.BigtableSession#createBulkMutation(BigtableTableName)} as a public
 * alternative.
 */
@InternalApi("For internal usage only - please use BulkMutation")
public class BulkMutationWrapper implements IBulkMutation {

  private final BulkMutation delegate;
  private boolean isClosed;

  public BulkMutationWrapper(BulkMutation bulkMutation) {
    this.delegate = bulkMutation;
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Void> add(RowMutationEntry rowMutation) {
    Preconditions.checkState(!isClosed, "can't mutate when the bulk mutation is closed.");
    return ApiFutureUtil.transformAndAdapt(
        delegate.add(rowMutation.toProto()),
        new Function<MutateRowResponse, Void>() {
          @Override
          public Void apply(MutateRowResponse response) {
            return null;
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public void sendUnsent() {
    delegate.sendUnsent();
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws InterruptedException {
    delegate.flush();
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
  }
}
