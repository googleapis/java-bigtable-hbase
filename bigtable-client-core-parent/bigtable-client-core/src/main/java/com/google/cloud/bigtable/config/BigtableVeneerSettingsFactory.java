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

import static com.google.api.client.util.Preconditions.checkState;
import static com.google.cloud.bigtable.config.BigtableOptions.BIGTABLE_EMULATOR_HOST_ENV_VAR;
import static io.grpc.internal.GrpcUtil.USER_AGENT_KEY;
import static org.threeten.bp.Duration.ofMillis;

import com.google.api.core.ApiFunction;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.stub.BigtableTableAdminStubSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import com.google.common.collect.ImmutableSet;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Set;
import javax.annotation.Nonnull;
import org.threeten.bp.Duration;

/**
 * Static methods to convert an instance of {@link BigtableOptions} to a
 * {@link BigtableDataSettings} or {@link BigtableTableAdminSettings} instance .
 */
public class BigtableVeneerSettingsFactory {

  /** Constant <code>LOG</code> */
  private static final Logger LOG = new Logger(BigtableVeneerSettingsFactory.class);

  //Identifier to distinguish between CBT or GCJ adapter.
  private static final String VENEER_ADAPTER =  BigtableVersionInfo.CORE_USER_AGENT+","
      + "VENEER_ADAPTER,";

  // 256 MB, server has 256 MB limit.
  private final static int MAX_MESSAGE_SIZE = 1 << 28;
  private static final int RPC_DEADLINE_MS = 360_000;
  private static final int MAX_RETRY_TIMEOUT_MS = 60_000;

  /**
   * To create an instance of {@link BigtableDataSettings} from {@link BigtableOptions}.
   *
   * @param options a {@link BigtableOptions} object.
   * @return a {@link BigtableDataSettings} object.
   * @throws IOException if any.
   */
  public static BigtableDataSettings createBigtableDataSettings(
      @Nonnull final BigtableOptions options) throws IOException {
    checkState(options.getRetryOptions().enableRetries(), "Disabling retries is not currently supported.");
    checkState(!options.useCachedChannel(), "cachedDataPool is not currently supported.");

    //TODO:add configuration for emulator hosting.
    String emulatorHost = System.getenv(BIGTABLE_EMULATOR_HOST_ENV_VAR);
    checkState(emulatorHost == null, "Emulator Hosting is not supported yet.");

    final BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder();
    Duration shortRpcTimeoutMs = ofMillis(options.getCallOptionsConfig().getShortRpcTimeoutMs());

    builder
        .setProjectId(options.getProjectId())
        .setInstanceId(options.getInstanceId())
        .setAppProfileId(options.getAppProfileId())
        .setEndpoint(options.getDataHost() + ":" + options.getPort())
        .setCredentialsProvider(buildCredentialProvider(options.getCredentialOptions()))
        .setHeaderProvider(buildHeaderProvider(options.getUserAgent()))
        .setTransportChannelProvider(buildChannelProvider(builder.getEndpoint(), options));

    // Configuration for rpcTimeout & totalTimeout for non-streaming operations.
    builder.checkAndMutateRowSettings()
        .setSimpleTimeoutNoRetries(shortRpcTimeoutMs);

    builder.readModifyWriteRowSettings()
        .setSimpleTimeoutNoRetries(shortRpcTimeoutMs);

    buildBulkMutationsSettings(builder, options);

    buildReadRowsSettings(builder, options);

    buildReadRowSettings(builder, options);

    buildMutateRowSettings(builder, options);

    buildSampleRowKeysSettings(builder, options);

    return builder.build();
  }

  /**
   * To create an instance of {@link BigtableTableAdminSettings} from {@link BigtableOptions}.
   *
   * @param options a {@link BigtableOptions} object.
   * @return a {@link BigtableTableAdminSettings} object.
   * @throws IOException if any.
   */
  public static BigtableTableAdminSettings createTableAdminSettings(
      @Nonnull final BigtableOptions options) throws IOException {
    final BigtableTableAdminSettings.Builder adminBuilder = BigtableTableAdminSettings.newBuilder();
    BigtableTableAdminStubSettings.Builder adminStub = adminBuilder.stubSettings();

    adminBuilder
        .setProjectId(options.getProjectId())
        .setInstanceId(options.getInstanceId());

    adminStub
        .setHeaderProvider(buildHeaderProvider(options.getUserAgent()))
        .setEndpoint(options.getAdminHost() + ":" + options.getPort())
        .setCredentialsProvider(buildCredentialProvider(options.getCredentialOptions()))
        .setTransportChannelProvider(buildChannelProvider(adminStub.getEndpoint(), options));

    return adminBuilder.build();
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

  /** Creates {@link HeaderProvider} with VENEER_ADAPTER as prefix for user agent */
  private static HeaderProvider buildHeaderProvider(String userAgent){
    return FixedHeaderProvider.create(USER_AGENT_KEY.name(), VENEER_ADAPTER + userAgent);
  }

  /** Builds {@link BatchingSettings} based on {@link BulkOptions} configuration. */
  private static void buildBulkMutationsSettings(Builder builder, BigtableOptions options) {
    BulkOptions bulkOptions = options.getBulkOptions();
    BatchingSettings.Builder batchBuilder =
        builder.bulkMutationsSettings().getBatchingSettings().toBuilder();

    long autoFlushMs = bulkOptions.getAutoflushMs();
    long bulkMaxRowKeyCount = bulkOptions.getBulkMaxRowKeyCount();
    long maxInflightRpcs = bulkOptions.getMaxInflightRpcs();

    if (autoFlushMs > 0) {
      batchBuilder.setDelayThreshold(ofMillis(autoFlushMs));
    }
    FlowControlSettings.Builder flowControlBuilder = FlowControlSettings.newBuilder();
    if (maxInflightRpcs > 0) {
      flowControlBuilder
          .setMaxOutstandingRequestBytes(bulkOptions.getMaxMemory())
          .setMaxOutstandingElementCount(maxInflightRpcs * bulkMaxRowKeyCount);
    }

    batchBuilder
        .setIsEnabled(bulkOptions.useBulkApi())
        .setElementCountThreshold(Long.valueOf(bulkOptions.getBulkMaxRowKeyCount()))
        .setRequestByteThreshold(bulkOptions.getBulkMaxRequestSize())
        .setFlowControlSettings(flowControlBuilder.build());

    RetrySettings retrySettings =
        buildIdempotentRetrySettings(builder.bulkMutationsSettings().getRetrySettings(), options);

    // TODO(rahulkql): implement bulkMutationThrottling & bulkMutationRpcTargetMs, once available
    builder.bulkMutationsSettings()
        .setBatchingSettings(batchBuilder.build())
        .setRetrySettings(retrySettings)
        .setRetryableCodes(buildRetryCodes(options.getRetryOptions()));
  }

  /** To build BigtableDataSettings#sampleRowKeysSettings with default Retry settings. */
  private static void buildSampleRowKeysSettings(Builder builder, BigtableOptions options) {
    RetrySettings retrySettings =
        buildIdempotentRetrySettings(builder.sampleRowKeysSettings().getRetrySettings(), options);

    builder.sampleRowKeysSettings()
        .setRetrySettings(retrySettings)
        .setRetryableCodes(buildRetryCodes(options.getRetryOptions()));
  }

  /** To build BigtableDataSettings#mutateRowSettings with default Retry settings. */
  private static void buildMutateRowSettings(Builder builder, BigtableOptions options) {
    RetrySettings retrySettings =
        buildIdempotentRetrySettings(builder.mutateRowSettings().getRetrySettings(), options);

    builder.mutateRowSettings()
        .setRetrySettings(retrySettings)
        .setRetryableCodes(buildRetryCodes(options.getRetryOptions()));
  }

  /** To build default Retry settings for Point Read. */
  private static void buildReadRowSettings(Builder builder, BigtableOptions options) {
    RetrySettings retrySettings =
        buildIdempotentRetrySettings(builder.readRowSettings().getRetrySettings(), options);

    builder.readRowSettings()
        .setRetrySettings(retrySettings)
        .setRetryableCodes(buildRetryCodes(options.getRetryOptions()));
  }

  /** To build BigtableDataSettings#readRowsSettings with default Retry settings. */
  private static void buildReadRowsSettings(Builder builder, BigtableOptions options) {
    RetryOptions retryOptions = options.getRetryOptions();
    CallOptionsConfig callOptions = options.getCallOptionsConfig();
    RetrySettings.Builder retryBuilder = builder.readRowsSettings().getRetrySettings().toBuilder();

    //Timeout for ReadRows
    Duration rpcTimeout = ofMillis(retryOptions.getReadPartialRowTimeoutMillis());
    Duration totalTimeout = ofMillis(callOptions.isUseTimeout()
        ? callOptions.getLongRpcTimeoutMs()
        : retryOptions.getMaxElapsedBackoffMillis());

    retryBuilder
        .setInitialRetryDelay(ofMillis(retryOptions.getInitialBackoffMillis()))
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
        .setMaxRetryDelay(ofMillis(MAX_RETRY_TIMEOUT_MS))
        .setMaxAttempts(retryOptions.getMaxScanTimeoutRetries())
        .setInitialRpcTimeout(rpcTimeout)
        .setMaxRpcTimeout(rpcTimeout)
        .setTotalTimeout(totalTimeout);

    builder.readRowsSettings()
        .setRetrySettings(retryBuilder.build())
        .setRetryableCodes(buildRetryCodes(options.getRetryOptions()));
  }

  /** Creates {@link RetrySettings} for non-streaming idempotent method. */
  private static RetrySettings buildIdempotentRetrySettings(RetrySettings retrySettings,
      BigtableOptions options) {
    RetryOptions retryOptions = options.getRetryOptions();
    CallOptionsConfig callOptions = options.getCallOptionsConfig();
    RetrySettings.Builder retryBuilder = retrySettings.toBuilder();

    if (retryOptions.allowRetriesWithoutTimestamp()) {
      LOG.warn("Retries without Timestamp does not support yet.");
    }

    // if useTimeout is false, then RPC's are defaults to 6 minutes.
    Duration rpcTimeout = ofMillis(callOptions.isUseTimeout()
        ? callOptions.getShortRpcTimeoutMs()
        : RPC_DEADLINE_MS);

    retryBuilder
        .setInitialRetryDelay(ofMillis(retryOptions.getInitialBackoffMillis()))
        .setRetryDelayMultiplier(retryOptions.getBackoffMultiplier())
        .setMaxRetryDelay(ofMillis(MAX_RETRY_TIMEOUT_MS))
        .setInitialRpcTimeout(rpcTimeout)
        .setMaxRpcTimeout(rpcTimeout)
        .setMaxAttempts(0)
        .setTotalTimeout(ofMillis(retryOptions.getMaxElapsedBackoffMillis()));

    return retryBuilder.build();
  }

  /** Creates {@link Set} of {@link StatusCode.Code} from {@link Status.Code} */
  private static Set<StatusCode.Code> buildRetryCodes(RetryOptions retryOptions) {
    ImmutableSet.Builder<StatusCode.Code> statusCodeBuilder = ImmutableSet.builder();
    for (Status.Code retryCode : retryOptions.getRetryableStatusCodes()) {
      statusCodeBuilder.add(GrpcStatusCode.of(retryCode).getCode());
    }

    return statusCodeBuilder.build();
  }

  /** Creates {@link TransportChannelProvider} based on Channel Negotiation type. */
  private static TransportChannelProvider buildChannelProvider(String endpoint,
      BigtableOptions options) {
    //TODO: refactor Google-cloud-java to expose a static defaultTransportChannelProvider.
    InstantiatingGrpcChannelProvider.Builder transportBuilder =
        InstantiatingGrpcChannelProvider.newBuilder()
            .setEndpoint(endpoint)
            .setPoolSize(options.getChannelCount())
            .setHeaderProvider(buildHeaderProvider(options.getUserAgent()))
            .setMaxInboundMessageSize(MAX_MESSAGE_SIZE);

    //overriding channel configuration for plaintext negotiation.
    if (options.usePlaintextNegotiation()) {
      transportBuilder.setChannelConfigurator(new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
            @Override
            public ManagedChannelBuilder apply(ManagedChannelBuilder channelBuilder) {
              return channelBuilder.usePlaintext();
            }
          });
    }
    return transportBuilder.build();
  }
}
