/*
 * Copyright 2020 Google LLC.
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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.hbase.wrappers.BulkMutationWrapper;
import com.google.cloud.bigtable.util.ApiFutureUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BulkMutationClassicApi implements BulkMutationWrapper {

  private final BulkMutation delegate;
  private boolean isClosed = false;

  BulkMutationClassicApi(BulkMutation delegate) {
    this.delegate = delegate;
  }

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

  @Override
  public void sendUnsent() {
    delegate.sendUnsent();
  }

  @Override
  public void flush() throws InterruptedException {
    delegate.flush();
  }

  @Override
  public void close() {
    isClosed = true;
  }
}
