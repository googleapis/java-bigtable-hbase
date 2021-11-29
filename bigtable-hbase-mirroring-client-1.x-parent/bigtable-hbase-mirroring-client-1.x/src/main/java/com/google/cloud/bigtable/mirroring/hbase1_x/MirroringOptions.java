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
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_CONNECTION_CONNECTION_TERMINATION_TIMEOUT;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FAILLOG_DROP_ON_OVERFLOW_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FAILLOG_MAX_BUFFER_SIZE_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FAILLOG_PREFIX_PATH_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_MAX_USED_BYTES;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_FLOW_CONTROLLER_STRATEGY_FACTORY_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_READ_VERIFICATION_RATE_PERCENT;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SCANNER_BUFFERED_MISMATCHED_READS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SYNCHRONOUS_WRITES;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_WRITE_ERROR_CONSUMER_FACTORY_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_WRITE_ERROR_LOG_APPENDER_FACTORY_CLASS;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_WRITE_ERROR_LOG_MAX_BINARY_VALUE_LENGTH;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_WRITE_ERROR_LOG_SERIALIZER_FACTORY_CLASS;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.DefaultSecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.Appender;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.DefaultAppender;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.DefaultSerializer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.faillog.Serializer;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowControlStrategy;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.RequestCountingFlowControlStrategy;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.DefaultMismatchDetector;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.MismatchDetector;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

@InternalApi("For internal use only")
public class MirroringOptions {
  public static class Faillog {
    Faillog(Configuration configuration) {
      this.prefixPath = configuration.get(MIRRORING_FAILLOG_PREFIX_PATH_KEY);
      this.maxBufferSize =
          configuration.getInt(MIRRORING_FAILLOG_MAX_BUFFER_SIZE_KEY, 20 * 1024 * 1024);
      this.dropOnOverflow = configuration.getBoolean(MIRRORING_FAILLOG_DROP_ON_OVERFLOW_KEY, false);
      this.writeErrorLogAppenderFactoryClass =
          configuration.getClass(
              MIRRORING_WRITE_ERROR_LOG_APPENDER_FACTORY_CLASS,
              DefaultAppender.Factory.class,
              Appender.Factory.class);
      this.writeErrorLogSerializerFactoryClass =
          configuration.getClass(
              MIRRORING_WRITE_ERROR_LOG_SERIALIZER_FACTORY_CLASS,
              DefaultSerializer.Factory.class,
              Serializer.Factory.class);
    }

    public final Class<? extends Appender.Factory> writeErrorLogAppenderFactoryClass;
    public final Class<? extends Serializer.Factory> writeErrorLogSerializerFactoryClass;
    public final String prefixPath;
    public final int maxBufferSize;
    public final boolean dropOnOverflow;
  }

  private static final String HBASE_CLIENT_WRITE_BUFFER_KEY = "hbase.client.write.buffer";

  public final Class<? extends MismatchDetector.Factory> mismatchDetectorFactoryClass;
  public final Class<? extends FlowControlStrategy.Factory> flowControllerStrategyFactoryClass;
  public final int flowControllerMaxOutstandingRequests;
  public final int flowControllerMaxUsedBytes;
  public final long bufferedMutatorBytesToFlush;
  public final Class<? extends SecondaryWriteErrorConsumer.Factory> writeErrorConsumerFactoryClass;
  public final int maxLoggedBinaryValueLength;
  public final int readSamplingRate;
  public final long connectionTerminationTimeoutMillis;

  public final boolean performWritesConcurrently;
  public final boolean waitForSecondaryWrites;
  public final Faillog faillog;

  public final int resultScannerBufferedMismatchedResults;

  public MirroringOptions(Configuration configuration) {
    this.mismatchDetectorFactoryClass =
        configuration.getClass(
            MIRRORING_MISMATCH_DETECTOR_FACTORY_CLASS,
            DefaultMismatchDetector.Factory.class,
            MismatchDetector.Factory.class);
    this.flowControllerStrategyFactoryClass =
        configuration.getClass(
            MIRRORING_FLOW_CONTROLLER_STRATEGY_FACTORY_CLASS,
            RequestCountingFlowControlStrategy.Factory.class,
            FlowControlStrategy.Factory.class);
    this.flowControllerMaxOutstandingRequests =
        configuration.getInt(MIRRORING_FLOW_CONTROLLER_MAX_OUTSTANDING_REQUESTS, 500);
    this.flowControllerMaxUsedBytes =
        configuration.getInt(MIRRORING_FLOW_CONTROLLER_MAX_USED_BYTES, 268435456);
    this.bufferedMutatorBytesToFlush =
        configuration.getInt(
            MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH,
            configuration.getInt(HBASE_CLIENT_WRITE_BUFFER_KEY, 2097152));
    this.writeErrorConsumerFactoryClass =
        configuration.getClass(
            MIRRORING_WRITE_ERROR_CONSUMER_FACTORY_CLASS,
            DefaultSecondaryWriteErrorConsumer.Factory.class,
            SecondaryWriteErrorConsumer.Factory.class);
    this.readSamplingRate = configuration.getInt(MIRRORING_READ_VERIFICATION_RATE_PERCENT, 100);
    Preconditions.checkArgument(this.readSamplingRate >= 0);
    Preconditions.checkArgument(this.readSamplingRate <= 100);

    this.performWritesConcurrently = configuration.getBoolean(MIRRORING_CONCURRENT_WRITES, false);
    this.waitForSecondaryWrites = configuration.getBoolean(MIRRORING_SYNCHRONOUS_WRITES, false);
    this.connectionTerminationTimeoutMillis =
        configuration.getLong(MIRRORING_CONNECTION_CONNECTION_TERMINATION_TIMEOUT, 60000);

    this.resultScannerBufferedMismatchedResults =
        configuration.getInt(MIRRORING_SCANNER_BUFFERED_MISMATCHED_READS, 5);
    Preconditions.checkArgument(this.resultScannerBufferedMismatchedResults >= 0);

    this.maxLoggedBinaryValueLength =
        configuration.getInt(MIRRORING_WRITE_ERROR_LOG_MAX_BINARY_VALUE_LENGTH, 32);
    Preconditions.checkArgument(this.maxLoggedBinaryValueLength >= 0);

    Preconditions.checkArgument(
        !(this.performWritesConcurrently && !this.waitForSecondaryWrites),
        "Performing writes concurrently and not waiting for writes is forbidden. "
            + "It has no advantage over performing writes asynchronously and not waiting for them.");
    this.faillog = new Faillog(configuration);
  }
}
