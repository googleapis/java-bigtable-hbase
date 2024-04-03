/*
 * Copyright 2024 Google LLC
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
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;

@InternalApi("For internal usage only")
public abstract class BigtableTable extends AbstractBigtableTable {
  private static Class<? extends BigtableTable> tableClass = null;

  public static BigtableTable create(
      AbstractBigtableConnection bigtableConnection, HBaseRequestAdapter hbaseAdapter) {
    try {
      return getSubclass()
          .getConstructor(AbstractBigtableConnection.class, HBaseRequestAdapter.class)
          .newInstance(bigtableConnection, hbaseAdapter);
    } catch (NoSuchMethodException
             | InstantiationException
             | IllegalAccessException
             | InvocationTargetException e) {
      throw new IllegalStateException("Failed to instantiate the proper subclass for Table", e);
    }
  }

  private static synchronized Class<? extends BigtableTable> getSubclass()
      throws NoSuchMethodException {
    if (tableClass != null) {
      return tableClass;
    }

    tableClass =
        new ByteBuddy()
            .subclass(BigtableTable.class)
            .name("com.google.cloud.bigtable.hbase1_x.BigtableTableImpl")
            .method(ElementMatchers.isAbstract())
            .intercept(InvocationHandlerAdapter.of(new UnsupportedOperationsHandler()))
            .make()
            .load(BigtableTable.class.getClassLoader())
            .getLoaded();

    return tableClass;
  }

  protected BigtableTable(
      AbstractBigtableConnection bigtableConnection, HBaseRequestAdapter hbaseAdapter) {
    super(bigtableConnection, hbaseAdapter);
  }

  // Implement the pre-hbase 2.4 mutateRow api
  @Override
  public void mutateRow(RowMutations rowMutations) throws IOException {
    mutateRowVoid(rowMutations);
  }
}
