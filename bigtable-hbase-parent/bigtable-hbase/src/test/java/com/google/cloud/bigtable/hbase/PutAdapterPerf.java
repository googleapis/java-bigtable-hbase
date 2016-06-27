/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.hbase.adapters.HBaseRequestAdapter;
import com.google.cloud.bigtable.hbase.adapters.PutAdapter;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple microbenchmark for {@link PutAdapter}
 */
public class PutAdapterPerf {
  public static void main(String[] args) {
    String rowKey = String.format("rowKey0");
    Put put = new Put(Bytes.toBytes(rowKey));
    byte[] value = RandomStringUtils.randomAlphanumeric(10000).getBytes();
    put.addColumn(Bytes.toBytes("Family1"), Bytes.toBytes("Qaulifier"), value);

    HBaseRequestAdapter adapter =
        new HBaseRequestAdapter(new BigtableOptions.Builder().build(),
            TableName.valueOf("tableName"), new Configuration());
    for (int i = 0; i < 10; i++) {
      putAdapterPerf(adapter, put);
    }
  }

  static int count = 100000;

  private static void putAdapterPerf(HBaseRequestAdapter adapter, Put put) {
    System.out.println("Size: " + adapter.adapt(put).getSerializedSize());
    System.gc();
    { 
      long start = System.nanoTime();
      for (int i = 0; i < count; i++) {
        adapter.adapt(put);
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format("RowMerger.readNext: %d rows merged in %d ms.  %d nanos per row.", count,
              time / 1000000, time / count));
    }
    System.gc();
    { 
      long start = System.nanoTime();
      for (int i = 0; i < count; i++) {
        MutateRowRequest adapted = adapter.adapt(put);
        BigtableGrpc.METHOD_MUTATE_ROW.streamRequest(adapted);
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format("RowMerger.readNext: %d rows merged in %d ms.  %d nanos per row.", count,
              time / 1000000, time / count));
    }
    System.gc();
    { 
      long start = System.nanoTime();
      for (int i = 0; i < count; i++) {
        MutateRowRequest adapted = adapter.adapt(put);
        adapted.getSerializedSize();
        BigtableGrpc.METHOD_MUTATE_ROW.streamRequest(adapted);
      }
      long time = System.nanoTime() - start;
      System.out.println(
          String.format("RowMerger.readNext / serialized size: %d rows merged in %d ms.  %d nanos per row.", count,
              time / 1000000, time / count));
    }
  }
}
