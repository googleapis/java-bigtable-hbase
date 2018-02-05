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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncBufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * Bigtable implementation of {@link AsyncBufferedMutator}
 * 
 * @author spollapally
 */
public class BigtableAsyncBufferedMutator implements AsyncBufferedMutator {

  @Override
  public void close() {
    throw new UnsupportedOperationException("close"); // TODO
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException("flush"); // TODO
  }

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException("getConfiguration"); // TODO
  }

  @Override
  public TableName getName() {
    throw new UnsupportedOperationException("getName"); // TODO
  }

  @Override
  public long getWriteBufferSize() {
    throw new UnsupportedOperationException("getWriteBufferSize"); // TODO
  }

  @Override
  public CompletableFuture<Void> mutate(Mutation arg0) {
    throw new UnsupportedOperationException("mutate"); // TODO
  }

  @Override
  public List<CompletableFuture<Void>> mutate(List<? extends Mutation> arg0) {
    throw new UnsupportedOperationException("mutate"); // TODO
  }
}
