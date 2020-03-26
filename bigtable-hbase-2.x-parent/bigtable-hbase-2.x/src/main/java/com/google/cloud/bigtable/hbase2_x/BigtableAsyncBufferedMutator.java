/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase2_x;

import static com.google.cloud.bigtable.hbase2_x.FutureUtils.toCompletableFuture;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.BigtableBufferedMutatorHelper;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncBufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Bigtable implementation of {@link AsyncBufferedMutator}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableAsyncBufferedMutator implements AsyncBufferedMutator {

  private final BigtableBufferedMutatorHelper helper;

  /**
   * Constructor for BigtableBufferedMutator.
   *
   * @param bigtableApi a {@link BigtableApi} object to access bigtable data client.
   * @param settings a {@link BigtableHBaseSettings} object for bigtable settings.
   * @param adapter a {@link HBaseRequestAdapter} object to convert HBase object to Bigtable protos.
   */
  public BigtableAsyncBufferedMutator(
      BigtableApi bigtableApi, BigtableHBaseSettings settings, HBaseRequestAdapter adapter) {
    helper = new BigtableBufferedMutatorHelper(bigtableApi, settings, adapter);
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    flush();
  }

  /** {@inheritDoc} */
  @Override
  public void flush() {
    helper.sendUnsent();
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConfiguration() {
    return helper.getConfiguration();
  }

  /** {@inheritDoc} */
  @Override
  public TableName getName() {
    return helper.getName();
  }

  /** {@inheritDoc} */
  @Override
  public long getWriteBufferSize() {
    return helper.getWriteBufferSize();
  }

  /** {@inheritDoc} */
  @Override
  public List<CompletableFuture<Void>> mutate(List<? extends Mutation> mutations) {
    return helper.mutate(mutations).stream()
        .map(listenableFuture -> toCompletableFuture(listenableFuture).thenApply(r -> (Void) null))
        .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Being a Mutation. This method will block if either of the following are true: 1) There are
   * more than {@code maxInflightRpcs} RPCs in flight 2) There are more than {@link
   * #getWriteBufferSize()} bytes pending
   */
  @Override
  public CompletableFuture<Void> mutate(final Mutation mutation) {
    return toCompletableFuture(helper.mutate(mutation)).thenApply(r -> null);
  }
}
