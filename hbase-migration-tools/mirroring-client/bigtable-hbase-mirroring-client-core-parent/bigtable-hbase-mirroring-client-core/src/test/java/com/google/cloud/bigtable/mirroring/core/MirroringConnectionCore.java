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
package com.google.cloud.bigtable.mirroring.core;

import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants;
import io.opencensus.common.Scope;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;

public class MirroringConnectionCore extends MirroringConnection {

  public MirroringConnectionCore(
      Configuration conf, boolean managed, ExecutorService pool, User user) throws Throwable {
    super(conf, managed, pool, user);
  }

  public MirroringConnectionCore(
      MirroringConfiguration mirroringConfiguration, ExecutorService pool) throws IOException {
    super(mirroringConfiguration, pool);
  }

  public Table getTable(final TableName tableName, ExecutorService executorService)
      throws IOException {
    try (Scope scope =
        this.mirroringTracer.spanFactory.operationScope(
            MirroringSpanConstants.HBaseOperation.GET_TABLE)) {
      Log.trace("getTable(%s, executorService)", tableName);
      Table primaryTable =
          this.mirroringTracer.spanFactory.wrapPrimaryOperation(
              () -> MirroringConnectionCore.this.primaryConnection.getTable(tableName),
              MirroringSpanConstants.HBaseOperation.GET_TABLE);
      Table secondaryTable = this.secondaryConnection.getTable(tableName);
      return new MirroringTableCore(
          primaryTable,
          secondaryTable,
          executorService,
          this.mismatchDetector,
          this.flowController,
          this.secondaryWriteErrorConsumer,
          this.readSampler,
          this.timestamper,
          this.performWritesConcurrently,
          this.waitForSecondaryWrites,
          this.mirroringTracer,
          this.referenceCounter,
          this.configuration.mirroringOptions.maxLoggedBinaryValueLength);
    }
  }
}
