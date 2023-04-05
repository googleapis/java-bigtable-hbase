/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.bigtable.hbase1_x;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.AbstractBigtableTable;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import net.bytebuddy.ByteBuddy;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.RowMutations;

@InternalApi
public abstract class BigtableTable extends AbstractBigtableTable {
  public BigtableTable(
      AbstractBigtableConnection bigtableConnection, HBaseRequestAdapter hbaseAdapter) {
    super(bigtableConnection, hbaseAdapter);
  }

  // Implement HBase 1 specific methods (in addition to what is implemented in
  // AbstractBigtableTable)
  // All other methods are implemented via bytecode generation to throw an error.
  // This allows the majority of to be supported across multiple hbase-client versions.
  @Override
  public void mutateRow(RowMutations rowMutations) throws IOException {
    mutateRowBase(rowMutations);
  }

  static BigtableTable create(
      AbstractBigtableConnection bigtableConnection, HBaseRequestAdapter hbaseAdapte) {
    try {
      return getSubclass()
          .getConstructor(AbstractBigtableConnection.class, HBaseRequestAdapter.class)
          .newInstance(bigtableConnection, hbaseAdapte);
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new IllegalStateException("Failed to create BigtableTable", e);
    }
  }

  private static Class<? extends BigtableTable> dynamicClass = null;

  private static Class<? extends BigtableTable> getSubclass() {
    if (dynamicClass != null) {
      return dynamicClass;
    }
    // default Result mutateRow(RowMutations rm) throws IOException {
    dynamicClass =
        new ByteBuddy()
            .subclass(BigtableTable.class)
            .make()
            .load(BigtableTable.class.getClassLoader())
            .getLoaded();

    return dynamicClass;
  }
}
