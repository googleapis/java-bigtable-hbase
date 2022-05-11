/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.security.User;

public class MirroringConnection
    extends com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection implements Connection {
  public MirroringConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws Throwable {
    super(conf, managed, pool, user);
  }

  public MirroringConnection(Configuration conf, ExecutorService pool, User user) throws Throwable {
    this(conf, false, pool, user);
  }

  @Override
  public void clearRegionLocationCache() {
    throw new UnsupportedOperationException("clearRegionLocationCache");
  }

  @Override
  public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService) {
    final TableBuilder primaryTableBuilder =
        getPrimaryConnection().getTableBuilder(tableName, executorService);
    final TableBuilder secondaryTableBuilder =
        getSecondaryConnection().getTableBuilder(tableName, executorService);
    return new TableBuilder() {
      @Override
      public TableBuilder setOperationTimeout(int timeout) {
        primaryTableBuilder.setOperationTimeout(timeout);
        secondaryTableBuilder.setOperationTimeout(timeout);
        return this;
      }

      @Override
      public TableBuilder setRpcTimeout(int timeout) {
        primaryTableBuilder.setRpcTimeout(timeout);
        secondaryTableBuilder.setRpcTimeout(timeout);
        return this;
      }

      @Override
      public TableBuilder setReadRpcTimeout(int timeout) {
        primaryTableBuilder.setReadRpcTimeout(timeout);
        secondaryTableBuilder.setReadRpcTimeout(timeout);
        return this;
      }

      @Override
      public TableBuilder setWriteRpcTimeout(int timeout) {
        primaryTableBuilder.setWriteRpcTimeout(timeout);
        secondaryTableBuilder.setWriteRpcTimeout(timeout);
        return this;
      }

      @Override
      public Table build() {
        return new MirroringTable(
            primaryTableBuilder.build(),
            secondaryTableBuilder.build(),
            executorService,
            mismatchDetector,
            flowController,
            secondaryWriteErrorConsumer,
            readSampler,
            timestamper,
            performWritesConcurrently,
            waitForSecondaryWrites,
            mirroringTracer,
            referenceCounter,
            MirroringConnection.super
                .configuration
                .mirroringOptions
                .resultScannerBufferedMismatchedResults);
      }
    };
  }
}
