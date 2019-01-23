/*
 * Copyright 2019 Google LLC.
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
package com.google.cloud.bigtable.config;

import com.google.api.core.ApiFunction;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.annotation.Nonnull;
import org.threeten.bp.Duration;

import static com.google.api.client.util.Preconditions.checkState;
import static com.google.cloud.bigtable.config.CallOptionsConfig.SHORT_TIMEOUT_MS_DEFAULT;
import static io.grpc.internal.GrpcUtil.USER_AGENT_KEY;
import static org.threeten.bp.Duration.ofMillis;

/**
 * Static methods to convert an instance of {@link BigtableOptions} to a
 * {@link BigtableDataSettings} or {@link BigtableTableAdminSettings} instance .
 */
public class BigtableVeneerSettingsFactory {

  /** Constant <code>LOG</code> */
  private static final Logger LOG = new Logger(BigtableVeneerSettingsFactory.class);

  //Identifier to distinguish between CBT or GCJ adapter.
  private static final String VENEER_ADAPTER =  BigtableVersionInfo.CORE_USER_AGENT+","
      + "VENEER_ADAPTER";

  // 256 MB, server has 256 MB limit.
  private final static int MAX_MESSAGE_SIZE = 1 << 28;
  /**
   * To create an instance of {@link BigtableDataSettings} from {@link BigtableOptions}.
   *
   * @param options a {@link BigtableOptions} object.
   * @return a {@link BigtableDataSettings} object.
   * @throws IOException if any.
   */
  public static BigtableDataSettings createBigtableDataSettings(@Nonnull final BigtableOptions options) throws IOException {
    checkState(options.getProjectId() != null, "Project ID is required");
    checkState(options.getInstanceId() != null, "Instance ID is required");
    checkState(options.getRetryOptions().enableRetries(), "Disabling retries is not currently supported.");

    final BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder();

    builder.setProjectId(options.getProjectId());
    builder.setInstanceId(options.getInstanceId());
    builder.setAppProfileId(options.getAppProfileId());

    builder.setEndpoint(options.getDataHost() + ":" + options.getPort());

    builder.setCredentialsProvider(buildCredentialProvider(options.getCredentialOptions()));

    buildBulkMutationsSettings(builder, options);

    buildCheckAndMutateRowSettings(builder, options.getCallOptionsConfig().getShortRpcTimeoutMs());

    buildReadModifyWriteSettings(builder, options.getCallOptionsConfig().getShortRpcTimeoutMs());

    buildReadRowSettings(builder, options);

    buildReadRowsSettings(builder, options);

    buildMutateRowSettings(builder, options);

    buildSampleRowKeysSettings(builder, options);

    builder.setTransportChannelProvider(createChannelProvider(options.getDataHost(), options));

    return builder.build();
  }

  /**
   * To create an instance of {@link BigtableTableAdminSettings} from {@link BigtableOptions}.
   *
   * @param options a {@link BigtableOptions} object.
   * @return a {@link BigtableTableAdminSettings} object.
   * @throws IOException if any.
   */
  public static BigtableTableAdminSettings createTableAdminSettings(@Nonnull final BigtableOptions options)
      throws IOException {
    checkState(options.getProjectId() != null, "Project ID is required");
    checkState(options.getInstanceId() != null, "Instance ID is required");

    final BigtableTableAdminSettings.Builder adminBuilder = BigtableTableAdminSettings.newBuilder();

    adminBuilder.setProjectId(options.getProjectId());
    adminBuilder.setInstanceId(options.getInstanceId());

    adminBuilder.stubSettings().setEndpoint(options.getAdminHost() + ":" + options.getPort());

    adminBuilder.stubSettings()
        .setCredentialsProvider(buildCredentialProvider(options.getCredentialOptions()));

    adminBuilder.stubSettings()
        .setTransportChannelProvider(createChannelProvider(options.getAdminHost(), options));

    return adminBuilder.build();
  }

  /** Builds {@link BatchingSettings} based on {@link BulkOptions} configuration. */
  private static void buildBulkMutationsSettings(Builder builder, BigtableOptions options) {
    BulkOptions bulkOptions = options.getBulkOptions();
    BatchingSettings.Builder batchSettingsBuilder = BatchingSettings.newBuilder();

    long autoFlushMs = bulkOptions.getAutoflushMs();
    long bulkMaxRowKeyCount = bulkOptions.getBulkMaxRowKeyCount();
    long maxInflightRpcs = bulkOptions.getMaxInflightRpcs();
    long shortRpcTimeoutMs = options.getCallOptionsConfig().getShortRpcTimeoutMs();

    if (autoFlushMs > 0) {
      batchSettingsBuilder.setDelayThreshold(ofMillis(autoFlushMs));
    }
    FlowControlSettings.Builder flowControlBuilder = FlowControlSettings.newBuilder();
    if (maxInflightRpcs > 0) {
      flowControlBuilder
          .setMaxOutstandingRequestBytes(bulkOptions.getMaxMemory())
          .setMaxOutstandingElementCount(maxInflightRpcs * bulkMaxRowKeyCount);
    }

    batchSettingsBuilder
        .setIsEnabled(bulkOptions.useBulkApi())
        .setElementCountThreshold(Long.valueOf(bulkOptions.getBulkMaxRowKeyCount()))
        .setRequestByteThreshold(bulkOptions.getBulkMaxRequestSize())
        .setFlowControlSettings(flowControlBuilder.build());

    // TODO(rahulkql): implement bulkMutationThrottling & bulkMutationRpcTargetMs, once available
    builder.bulkMutationsSettings()
        .setBatchingSettings(batchSettingsBuilder.build())
        .setSimpleTimeoutNoRetries(
            ofMillis(shortRpcTimeoutMs));
  }

  /** To build BigtableDataSettings#sampleRowKeysSettings with default Retry settings. */
  private static void buildSampleRowKeysSettings(Builder builder, BigtableOptions options) {
    builder.sampleRowKeysSettings()
        .setRetrySettings(buildIdempotentRetrySettings(options));
  }

  /** To build BigtableDataSettings#mutateRowSettings with default Retry settings. */
  private static void buildMutateRowSettings(Builder builder, BigtableOptions options) {
    builder.mutateRowSettings()
        .setRetrySettings(buildIdempotentRetrySettings(options));
  }

  /** To build default Retry settings for Point Read. */
  private static void buildReadRowSettings(Builder builder, BigtableOptions options) {
    RetryOptions retryOptions = options.getRetryOptions();

    RetrySettings.Builder retryBuilder = RetrySettings.newBuilder()
        .setInitialRetryDelay(ofMillis(retryOptions.getInitialBackoffMillis()))
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
        .setMaxRetryDelay(ofMillis(retryOptions.getMaxElapsedBackoffMillis()))
        .setMaxAttempts(retryOptions.getMaxScanTimeoutRetries());

    // configurations for RPC timeouts
    Duration readPartialRowTimeout = ofMillis(retryOptions.getReadPartialRowTimeoutMillis());
    retryBuilder
        .setInitialRpcTimeout(readPartialRowTimeout)
        .setMaxRpcTimeout(readPartialRowTimeout)
        .setTotalTimeout(ofMillis(options.getCallOptionsConfig().getLongRpcTimeoutMs()));

    builder.readRowSettings()
        .setRetrySettings(retryBuilder.build());
  }

  /** To build BigtableDataSettings#readRowsSettings with default Retry settings. */
  private static void buildReadRowsSettings(Builder builder, BigtableOptions options) {
    RetryOptions retryOptions = options.getRetryOptions();

    RetrySettings.Builder retryBuilder = RetrySettings.newBuilder()
        .setInitialRetryDelay(ofMillis(retryOptions.getInitialBackoffMillis()))
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
        .setMaxRetryDelay(ofMillis(retryOptions.getMaxElapsedBackoffMillis()))
        .setMaxAttempts(retryOptions.getMaxScanTimeoutRetries());

    // configurations for RPC timeouts
    Duration readPartialRowTimeout = ofMillis(retryOptions.getReadPartialRowTimeoutMillis());
    retryBuilder
        .setInitialRpcTimeout(readPartialRowTimeout)
        .setMaxRpcTimeout(readPartialRowTimeout)
        .setTotalTimeout(ofMillis(options.getCallOptionsConfig().getLongRpcTimeoutMs()));

    builder.readRowsSettings()
        .setRetrySettings(retryBuilder.build());
  }

  /**
   * Builds BigtableDataSettings#readModifyWriteRowSettings when short timeout is other
   * than 60_000 ms.
   */
  private static void buildReadModifyWriteSettings(Builder builder, long rpcTimeoutMs) {
    if(rpcTimeoutMs != SHORT_TIMEOUT_MS_DEFAULT) {
      builder.readModifyWriteRowSettings()
          .setSimpleTimeoutNoRetries(ofMillis(rpcTimeoutMs));
    }
  }

  /**
   * Builds BigtableDataSettings#checkAndMutateRowSettings when short timeout is other
   * than 60_000 ms.
   */
  private static void buildCheckAndMutateRowSettings(Builder builder, long rpcTimeoutMs) {
    if(rpcTimeoutMs != SHORT_TIMEOUT_MS_DEFAULT) {
      builder.checkAndMutateRowSettings()
          .setSimpleTimeoutNoRetries(ofMillis(rpcTimeoutMs));
    }
  }

  /** Creates default {@link RetrySettings} for all idempotent method. */
  private static RetrySettings buildIdempotentRetrySettings(BigtableOptions options) {
    RetryOptions retryOptions = options.getRetryOptions();

    RetrySettings.Builder retryBuilder = RetrySettings.newBuilder()
        .setInitialRetryDelay(ofMillis(retryOptions.getInitialBackoffMillis()))
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
        .setMaxRetryDelay(ofMillis(retryOptions.getMaxElapsedBackoffMillis()))
        .setMaxAttempts(retryOptions.getMaxScanTimeoutRetries());

    // configurations for RPC timeouts
    Duration shortRpcTimeout = ofMillis(options.getCallOptionsConfig().getShortRpcTimeoutMs());
    retryBuilder
        .setInitialRpcTimeout(shortRpcTimeout)
        .setMaxRpcTimeout(shortRpcTimeout)
        .setTotalTimeout(ofMillis(options.getCallOptionsConfig().getLongRpcTimeoutMs()));

    if (retryOptions.allowRetriesWithoutTimestamp()) {
      LOG.warn("Retries without Timestamp does not support yet.");
    }

    return retryBuilder.build();
  }

  /** Creates {@link CredentialsProvider} based on {@link CredentialOptions}. */
  private static CredentialsProvider buildCredentialProvider(
      CredentialOptions credentialOptions) throws IOException {
    try {
      final Credentials credentials = CredentialFactory.getCredentials(credentialOptions);
      if (credentials == null) {
        LOG.info("Enabling the use of null credentials. This should not be used in production.");
        return NoCredentialsProvider.create();
      }

      return FixedCredentialsProvider.create(credentials);
    } catch (GeneralSecurityException exception) {
      throw new IOException("Could not initialize credentials.", exception);
    }
  }

  /** Creates {@link TransportChannelProvider} based on Channel Negotiation type. */
  private static TransportChannelProvider createChannelProvider(String hostname,
      BigtableOptions options) {
    final String endpoint = hostname + ":" + options.getPort();
    String userAgent = VENEER_ADAPTER + options.getUserAgent();

    //InstantiatingGrpcChannelProvider has special handling for User-Agent header. It set the
    //ChannelBuilder with provided value.
    HeaderProvider headers = FixedHeaderProvider.create(USER_AGENT_KEY.name(), userAgent);

    InstantiatingGrpcChannelProvider.Builder builder =
        InstantiatingGrpcChannelProvider.newBuilder()
            .setChannelsPerCpu(2)
            .setHeaderProvider(headers)
            .setEndpoint(endpoint)
            .setPoolSize(options.getChannelCount())
            .setMaxInboundMessageSize(MAX_MESSAGE_SIZE);

    //overriding channel configuration for plaintext negotiation.
    if (options.usePlaintextNegotiation()) {
      builder.setChannelConfigurator(
          new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
            @Override
            public ManagedChannelBuilder apply(ManagedChannelBuilder channelBuilder) {
              return channelBuilder.usePlaintext();
            }
          });
    }

    return builder.build();
  }
}
