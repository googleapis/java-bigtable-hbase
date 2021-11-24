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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_CONCURRENT_WRITES;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_MAX_USED_BYTES;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_STRATEGY_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_MISMATCH_DETECTOR_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_READ_VERIFICATION_RATE_PERCENT;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SYNCHRONOUS_WRITES;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_WRITE_ERROR_CONSUMER_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_WRITE_ERROR_LOG_APPENDER_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_WRITE_ERROR_LOG_MAX_BINARY_VALUE_LENGTH;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_WRITE_ERROR_LOG_SERIALIZER_CLASS;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.DefaultSecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.DefaultAppender;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.DefaultSerializer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestCountingFlowControlStrategy;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.DefaultMismatchDetector;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

@InternalApi("For internal use only")
public class MirroringOptions {
  private static final String HBASE_CLIENT_WRITE_BUFFER_KEY = "hbase.client.write.buffer";
  public final String mismatchDetectorClass;
  public final String flowControllerStrategyClass;
  public final int flowControllerMaxOutstandingRequests;
  public final int flowControllerMaxUsedBytes;
  public final long bufferedMutatorBytesToFlush;
  public final String writeErrorConsumerClass;
  public final int maxLoggedBinaryValueLength;
  public final int readSamplingRate;

  public final String writeErrorLogAppenderClass;
  public final String writeErrorLogSerializerClass;

  public final boolean performWritesConcurrently;
  public final boolean waitForSecondaryWrites;

  public MirroringOptions(Configuration configuration) {
    this.mismatchDetectorClass =
        configuration.get(
            MIRRORING_MISMATCH_DETECTOR_CLASS, DefaultMismatchDetector.class.getCanonicalName());
    this.flowControllerStrategyClass =
        configuration.get(
            MIRRORING_FLOW_CONTROLLER_STRATEGY_CLASS,
            RequestCountingFlowControlStrategy.class.getCanonicalName());
    this.flowControllerMaxOutstandingRequests =
        Integer.parseInt(
            configuration.get(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, "500"));
    this.flowControllerMaxUsedBytes =
        Integer.parseInt(configuration.get(MIRRORING_FLOW_CONTROLLER_MAX_USED_BYTES, "268435456"));
    this.bufferedMutatorBytesToFlush =
        Integer.parseInt(
            configuration.get(
                MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH,
                configuration.get(HBASE_CLIENT_WRITE_BUFFER_KEY, "2097152")));
    this.writeErrorConsumerClass =
        configuration.get(
            MIRRORING_WRITE_ERROR_CONSUMER_CLASS,
            DefaultSecondaryWriteErrorConsumer.class.getCanonicalName());
    this.readSamplingRate =
        Integer.parseInt(configuration.get(MIRRORING_READ_VERIFICATION_RATE_PERCENT, "100"));
    Preconditions.checkArgument(this.readSamplingRate >= 0);
    Preconditions.checkArgument(this.readSamplingRate <= 100);
    this.writeErrorLogAppenderClass =
        configuration.get(
            MIRRORING_WRITE_ERROR_LOG_APPENDER_CLASS, DefaultAppender.class.getCanonicalName());
    this.writeErrorLogSerializerClass =
        configuration.get(
            MIRRORING_WRITE_ERROR_LOG_SERIALIZER_CLASS, DefaultSerializer.class.getCanonicalName());
    this.maxLoggedBinaryValueLength =
        configuration.getInt(MIRRORING_WRITE_ERROR_LOG_MAX_BINARY_VALUE_LENGTH, 32);
    this.performWritesConcurrently = configuration.getBoolean(MIRRORING_CONCURRENT_WRITES, false);
    this.waitForSecondaryWrites = configuration.getBoolean(MIRRORING_SYNCHRONOUS_WRITES, false);

    Preconditions.checkArgument(
        !(this.performWritesConcurrently && !this.waitForSecondaryWrites),
        "Performing writes concurrently and not waiting for writes is forbidden. "
            + "It has no advantage over performing writes asynchronously and not waiting for them.");
  }
}
