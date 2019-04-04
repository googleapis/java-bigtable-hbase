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
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.models.BulkMutationBatcher;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is meant to replicate existing {@link BulkMutation} while translating calls to
 * Google-Cloud-Java's {@link BulkMutationBatcher} api.
 */
public class BulkMutationGCJClient implements IBulkMutation {

  // Maximum wait time for all outstanding response futures in either flush() or close() to resolve.
  private static final long MAX_RPC_WAIT_TIME = 12;

  private final BulkMutationBatcher bulkMutateBatcher;
  private List<ApiFuture<Void>> responseFutures = new ArrayList<>();
  private ReentrantLock lock = new ReentrantLock();

  public BulkMutationGCJClient(BulkMutationBatcher bulkMutateBatcher) {
    this.bulkMutateBatcher = bulkMutateBatcher;
  }

  /** {@inheritDoc} */
  @Override
  public void flush() {
    lock.lock();
    try {
      List<ApiFuture<Void>> accumulatedResFuture = responseFutures;
      responseFutures = new ArrayList<>();
      ApiFutures.allAsList(accumulatedResFuture).get(MAX_RPC_WAIT_TIME, TimeUnit.MINUTES);
    } catch (Exception e) {
      throw new RuntimeException("Something happened with RPC results", e);
    } finally {
      lock.unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void sendUnsent() {
    if (!isFlushed()) {
      flush();
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean isFlushed() {
    lock.lock();
    try {
      return responseFutures.isEmpty();
    } finally {
      lock.unlock();
    }
  }

  /** {@inheritDoc} */
  @Override
  public ApiFuture<Void> add(RowMutation rowMutation) {
    lock.lock();
    try {
      final ApiFuture<Void> res = bulkMutateBatcher.add(rowMutation);
      responseFutures.add(res);
      ApiFutures.addCallback(res, new ApiFutureCallback<Void>() {
        @Override
        public void onFailure(Throwable t) {
          responseFutures.remove(res);
        }

        @Override
        public void onSuccess(Void result) {
          responseFutures.remove(res);
        }
      }, MoreExecutors.directExecutor());
      return res;
    } finally {
      lock.unlock();
    }
  }
}
