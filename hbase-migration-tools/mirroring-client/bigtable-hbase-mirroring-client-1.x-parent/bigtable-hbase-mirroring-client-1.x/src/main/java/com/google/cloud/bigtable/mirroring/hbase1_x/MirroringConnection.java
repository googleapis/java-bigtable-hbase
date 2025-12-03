/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import com.google.cloud.bigtable.mirroring.core.MirroringConfiguration;
import com.google.cloud.bigtable.mirroring.core.utils.ReadSampler;
import com.google.cloud.bigtable.mirroring.core.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
import com.google.cloud.bigtable.mirroring.core.verification.MismatchDetector;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;

public class MirroringConnection
    extends com.google.cloud.bigtable.mirroring.core.MirroringConnection {

  /**
   * The constructor called from {@link ConnectionFactory#createConnection(Configuration)} and in
   * its many forms via reflection with this specific signature.
   *
   * <p>Parameters are passed down to ConnectionFactory#createConnection method, connection errors
   * are passed back to the user.
   */
  public MirroringConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws Throwable {
    super(conf, managed, pool, user);
  }

  public MirroringConnection(MirroringConfiguration mirroringConfiguration, ExecutorService pool)
      throws IOException {
    super(mirroringConfiguration, pool);
  }

  @Override
  protected Table getMirroringTable(Table primaryTable, Table secondaryTable,
      ExecutorService executorService, MismatchDetector mismatchDetector,
      FlowController flowController, SecondaryWriteErrorConsumer secondaryWriteErrorConsumer,
      ReadSampler readSampler, Timestamper timestamper, boolean performWritesConcurrently,
      boolean waitForSecondaryWrites, MirroringTracer mirroringTracer,
      ReferenceCounter parentReferenceCounter, int resultScannerBufferedMismatchedResults) {
    return new MirroringTable(
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
