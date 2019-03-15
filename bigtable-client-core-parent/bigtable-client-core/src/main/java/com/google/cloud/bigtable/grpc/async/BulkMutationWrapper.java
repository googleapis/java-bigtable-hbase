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
import com.google.bigtable.v2.MutateRowResponse;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.util.ApiFutureUtil;
import com.google.common.base.Function;

/**
 * This class wraps existing {@link com.google.cloud.bigtable.grpc.async.BulkMutation} with
 * Google-cloud-java's model.
 */
public class BulkMutationWrapper implements IBulkMutation {

  private final BulkMutation delegate;
  private final RequestContext requestContext;

  public BulkMutationWrapper(BulkMutation bulkMutation, RequestContext requestContext) {
    this.delegate = bulkMutation;
    this.requestContext = requestContext;
  }

  /** {@inheritDoc} */
  @Override
  public void flush() throws InterruptedException {
    delegate.flush();
  }

  /** {@inheritDoc} */
  @Override
  public void sendUnsent() {
    delegate.sendUnsent();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isFlushed() {
    return delegate.isFlushed();
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Void> add(RowMutation rowMutation) {
    return ApiFutureUtil.transformAndAdapt(delegate.add(rowMutation.toBulkProto(requestContext).getEntries(0)),
        new Function<MutateRowResponse, Void>() {
          @Override
          public Void apply(MutateRowResponse response) {
            return null;
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public void register(ApiFuture<?> future) {
    delegate.register(ApiFutureUtil.adapt(future));
  }
}
