/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v2.MutateRowRequest;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.HashMap;
import java.util.Map;

/**
 * Adapt a generic Mutation to a bigtable operation.
 *
 * This class uses instanceof checking to determine an appropriate adaptation to apply.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class MutationAdapter
    implements OperationAdapter<Mutation, MutateRowRequest.Builder> {

  static class AdapterInstanceMap {
    private Map<Class<?>, OperationAdapter<?, ?>> unsafeMap =
        new HashMap<Class<?>, OperationAdapter<?, ?>>();

    public <S extends Mutation, U extends OperationAdapter<S, MutateRowRequest.Builder>>
    Class<S> put(Class<S> key, U adapter) {
      unsafeMap.put(key, adapter);
      return key;
    }

    // The only way to add to the unsafeMap is via put which enforces our type constraints at
    // compile-time. The unchecked cast should be safe.
    @SuppressWarnings("unchecked")
    public <S extends Mutation, U extends OperationAdapter<S, MutateRowRequest.Builder>>
    U get(Class<? extends S> key) {
      return (U) unsafeMap.get(key);
    }
  }

  private final AdapterInstanceMap adapterMap = new AdapterInstanceMap();

  /**
   * <p>Constructor for MutationAdapter.</p>
   *
   * @param deleteAdapter a {@link com.google.cloud.bigtable.hbase.adapters.OperationAdapter} object.
   * @param putAdapter a {@link com.google.cloud.bigtable.hbase.adapters.OperationAdapter} object.
   * @param incrementAdapter a {@link com.google.cloud.bigtable.hbase.adapters.OperationAdapter} object.
   * @param appendAdapter a {@link com.google.cloud.bigtable.hbase.adapters.OperationAdapter} object.
   */
  public MutationAdapter(
      OperationAdapter<Delete, MutateRowRequest.Builder> deleteAdapter,
      OperationAdapter<Put, MutateRowRequest.Builder> putAdapter,
      OperationAdapter<Increment, MutateRowRequest.Builder> incrementAdapter,
      OperationAdapter<Append, MutateRowRequest.Builder> appendAdapter) {
    adapterMap.put(Delete.class, deleteAdapter);
    adapterMap.put(Put.class, putAdapter);
    adapterMap.put(Increment.class, incrementAdapter);
    adapterMap.put(Append.class, appendAdapter);
  }

  /** {@inheritDoc} */
  @Override
  public MutateRowRequest.Builder adapt(Mutation mutation) {
    OperationAdapter<Mutation, MutateRowRequest.Builder> adapter =
        adapterMap.get(mutation.getClass());
    if (adapter == null) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot adapt mutation of type %s.", mutation.getClass().getCanonicalName()));
    }
    return adapter.adapt(mutation);
  }
}
