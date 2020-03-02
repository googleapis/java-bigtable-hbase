/*
 * Copyright 2020 Google LLC.
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
package com.google.cloud.bigtable.hbase.wrappers.veneer;

import static com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ADDITIONAL_RETRY_CODES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ALLOW_NO_TIMESTAMP_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.APP_PROFILE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_AUTOFLUSH_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_PORT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ENABLE_GRPC_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_INFLIGHT_RPCS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_PARTIAL_ROW_TIMEOUT_MS;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.grpc.internal.GrpcUtil.USER_AGENT_KEY;
import static org.threeten.bp.Duration.ofMillis;

import com.google.api.client.util.SecurityUtils;
import com.google.api.core.ApiFunction;
import com.google.api.core.InternalApi;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StubSettings;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.stub.BigtableInstanceAdminStubSettings;
import com.google.cloud.bigtable.admin.v2.stub.BigtableTableAdminStubSettings;
import com.google.cloud.bigtable.config.BigtableVersionInfo;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.cloud.bigtable.hbase.BigtableExtendedConfiguration;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.grpc.ManagedChannelBuilder;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.threeten.bp.Duration;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableHBaseVeneerSettings extends BigtableHBaseSettings {

  // Identifier to distinguish between CBT or GCJ adapter.
  private static final String VENEER_ADAPTER =
      BigtableVersionInfo.CORE_USER_AGENT + "," + "veneer-adapter,";

  // Keeping the original configuration, this could be an instance of BigtableExtendedConfiguration.
  private final Configuration configuration;
  private final BigtableDataSettings dataSettings;
  private final BigtableTableAdminSettings tableAdminSettings;
  @Nullable private final BigtableInstanceAdminSettings instanceAdminSettings;

  public BigtableHBaseVeneerSettings(Configuration configuration) throws IOException {
    super(configuration);
    this.configuration = configuration;
    this.dataSettings = buildBigtableDataSettings();
    this.tableAdminSettings = buildBigtableTableAdminSettings();

    if (!isNullOrEmpty(configuration.get(BIGTABLE_EMULATOR_HOST_KEY))) {
      LOG.info("bigtable emulator does not support Instance Admin client.");
      instanceAdminSettings = null;
    } else {
      instanceAdminSettings = buildBigtableInstanceAdminSettings();
    }
  }

  @Override
  public String getDataHost() {
    String endpoint = dataSettings.getStubSettings().getEndpoint();
    return endpoint.substring(0, endpoint.lastIndexOf(":"));
  }

  @Override
  public String getAdminHost() {
    String endpoint = tableAdminSettings.getStubSettings().getEndpoint();
    return endpoint.substring(0, endpoint.lastIndexOf(":"));
  }

  @Override
  public int getPort() {
    String endpoint = dataSettings.getStubSettings().getEndpoint();
    return Integer.parseInt(endpoint.substring(endpoint.lastIndexOf(":") + 1));
  }

  @Override
  public int getBulkMaxRowCount() {
    return dataSettings
        .getStubSettings()
        .bulkMutateRowsSettings()
        .getBatchingSettings()
        .getElementCountThreshold()
        .intValue();
  }

  // ************** Getters **************
  public boolean isChannelPoolCachingEnabled() {
    // This is primarily used by Dataflow where connections open and close often. This is a
    // performance optimization that will reduce the cost to open connections.
    return configuration.getBoolean(BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, false);
  }

  /** Utility to convert {@link Configuration} to {@link BigtableDataSettings}. */
  public BigtableDataSettings getDataSettings() {
    return dataSettings;
  }

  /** Utility to convert {@link Configuration} to {@link BigtableTableAdminSettings}. */
  public BigtableTableAdminSettings getTableAdminSettings() {
    return tableAdminSettings;
  }

  /** Utility to convert {@link Configuration} to {@link BigtableInstanceAdminSettings}. */
  @Nullable
  public BigtableInstanceAdminSettings getInstanceAdminSettings() {
    return instanceAdminSettings;
  }

  // ************** Private Helpers **************
  private BigtableDataSettings buildBigtableDataSettings() throws IOException {
    BigtableDataSettings.Builder dataBuilder =
        BigtableDataSettings.newBuilder()
            .setProjectId(getProjectId())
            .setInstanceId(getInstanceId());

    String appProfileId = configuration.get(APP_PROFILE_ID_KEY);
    if (!isNullOrEmpty(appProfileId)) {
      dataBuilder.setAppProfileId(appProfileId);
    }

    EnhancedBigtableStubSettings.Builder stubSettings = dataBuilder.stubSettings();

    buildEndpoints(stubSettings, BIGTABLE_HOST_KEY);

    buildHeaderProvider(stubSettings);

    buildCredentialProvider(stubSettings);

    // RPC methods
    buildBulkMutationsSettings(stubSettings);

    buildBulkReadRowsSettings(stubSettings);

    buildReadRowsSettings(stubSettings);

    buildNonIdempotentCallSettings(stubSettings.checkAndMutateRowSettings());
    buildNonIdempotentCallSettings(stubSettings.readModifyWriteRowSettings());

    buildIdempotentCallSettings(stubSettings.readRowSettings());
    buildIdempotentCallSettings(stubSettings.mutateRowSettings());
    buildIdempotentCallSettings(stubSettings.sampleRowKeysSettings());

    buildEmulatorSettings(stubSettings);

    return dataBuilder.build();
  }

  private BigtableTableAdminSettings buildBigtableTableAdminSettings() throws IOException {
    BigtableTableAdminSettings.Builder adminBuilder =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(getProjectId())
            .setInstanceId(getInstanceId());

    BigtableTableAdminStubSettings.Builder stubSettings = adminBuilder.stubSettings();

    buildEndpoints(stubSettings, BIGTABLE_ADMIN_HOST_KEY);

    buildHeaderProvider(stubSettings);

    buildCredentialProvider(stubSettings);

    buildEmulatorSettings(stubSettings);

    return adminBuilder.build();
  }

  private BigtableInstanceAdminSettings buildBigtableInstanceAdminSettings() throws IOException {
    BigtableInstanceAdminSettings.Builder instanceAdminBuilder =
        BigtableInstanceAdminSettings.newBuilder().setProjectId(getProjectId());

    BigtableInstanceAdminStubSettings.Builder stubSettings = instanceAdminBuilder.stubSettings();

    buildEndpoints(stubSettings, BIGTABLE_ADMIN_HOST_KEY);

    buildHeaderProvider(stubSettings);

    buildCredentialProvider(stubSettings);

    return instanceAdminBuilder.build();
  }

  private void buildEndpoints(StubSettings.Builder stubSettings, String endpointKey) {
    String adminHostOverride = configuration.get(endpointKey);
    if (!isNullOrEmpty(adminHostOverride)) {

      String port = configuration.get(BIGTABLE_PORT_KEY);
      if (isNullOrEmpty(port)) {
        String endpoint = stubSettings.getEndpoint();
        port = endpoint.substring(endpoint.lastIndexOf(":") + 1);
      }

      String finalEndpoint = adminHostOverride + ":" + port;
      LOG.debug("%s is configured at %s", endpointKey, finalEndpoint);
      stubSettings.setEndpoint(finalEndpoint);
    }

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_PLAINTEXT_NEGOTIATION))) {
      stubSettings.setTransportChannelProvider(
          buildPlainTextChannelProvider(stubSettings.getEndpoint()));
    }
  }

  /** Creates {@link HeaderProvider} with VENEER_ADAPTER as prefix for user agent */
  private void buildHeaderProvider(StubSettings.Builder stubSettings) {

    // This information is in addition to bigtable-client-core version, and jdk version.
    StringBuilder agentBuilder = new StringBuilder();
    agentBuilder.append("hbase-").append(VersionInfo.getVersion());
    String customUserAgent = configuration.get(CUSTOM_USER_AGENT_KEY);
    if (customUserAgent != null) {
      agentBuilder.append(',').append(customUserAgent);
    }

    stubSettings.setHeaderProvider(
        FixedHeaderProvider.create(
            USER_AGENT_KEY.name(), VENEER_ADAPTER + agentBuilder.toString()));
  }

  private void buildCredentialProvider(StubSettings.Builder stubSettings) throws IOException {
    Credentials credentials = null;

    if (configuration instanceof BigtableExtendedConfiguration) {
      credentials = ((BigtableExtendedConfiguration) configuration).getCredentials();

    } else if (Boolean.parseBoolean(configuration.get(BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY))) {
      stubSettings.setCredentialsProvider(NoCredentialsProvider.create());
      LOG.info("Enabling the use of null credentials. This should not be used in production.");

    } else if (!isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY))) {
      String jsonValue = configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY);
      LOG.debug("Using json value");
      Preconditions.checkState(
          !isNullOrEmpty(jsonValue), "service account json value is null or empty");
      credentials =
          GoogleCredentials.fromStream(
              new ByteArrayInputStream(jsonValue.getBytes(StandardCharsets.UTF_8)));

    } else if (!isNullOrEmpty(
        configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY))) {
      String keyFileLocation =
          configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY);
      LOG.debug("Using json keyfile: %s", keyFileLocation);
      Preconditions.checkState(
          !isNullOrEmpty(keyFileLocation), "service account location is null or empty");
      credentials = GoogleCredentials.fromStream(new FileInputStream(keyFileLocation));

    } else if (!isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY))) {

      String serviceAccount = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
      LOG.debug("Service account %s specified.", serviceAccount);
      String keyFileLocation = configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
      Preconditions.checkState(
          !isNullOrEmpty(keyFileLocation),
          "Key file location must be specified when setting service account email");
      LOG.debug("Using p12 keyfile: %s", keyFileLocation);

      credentials = getCredentialFromPrivateKeyServiceAccount(serviceAccount, keyFileLocation);
    }

    if (credentials != null) {
      stubSettings.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    }
  }

  // copied over from CredentialFactory
  // TODO: Find a better way to convert P12 key into Credentials instance
  private Credentials getCredentialFromPrivateKeyServiceAccount(
      String serviceAccountEmail, String privateKeyFile) throws IOException {
    try {
      PrivateKey privateKey =
          SecurityUtils.loadPrivateKeyFromKeyStore(
              SecurityUtils.getPkcs12KeyStore(),
              new FileInputStream(privateKeyFile),
              "notasecret",
              "privatekey",
              "notasecret");

      return ServiceAccountJwtAccessCredentials.newBuilder()
          .setClientEmail(serviceAccountEmail)
          .setPrivateKey(privateKey)
          .build();
    } catch (GeneralSecurityException exception) {
      throw new RuntimeException("exception while retrieving credentials", exception);
    }
  }

  /** Creates {@link TransportChannelProvider} for plaintext negotiation type. */
  private TransportChannelProvider buildPlainTextChannelProvider(String endpoint) {

    InstantiatingGrpcChannelProvider.Builder channelBuilder =
        defaultGrpcTransportProviderBuilder()
            .setEndpoint(endpoint)
            .setChannelConfigurator(
                new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
                  @Override
                  public ManagedChannelBuilder apply(ManagedChannelBuilder channelBuilder) {
                    return channelBuilder.usePlaintext();
                  }
                });

    String channelCount = configuration.get(BIGTABLE_DATA_CHANNEL_COUNT_KEY);
    if (!isNullOrEmpty(channelCount)) {
      channelBuilder.setPoolSize(Integer.parseInt(channelCount));
    }

    return channelBuilder.build();
  }

  private Set<StatusCode.Code> extractRetryCodesFromConfig() {
    ImmutableSet.Builder<StatusCode.Code> statusCodeBuilder = ImmutableSet.builder();
    String retryCodes = configuration.get(ADDITIONAL_RETRY_CODES, "");

    for (String stringCode : retryCodes.split(",")) {
      String trimmed = stringCode.trim();
      if (trimmed.isEmpty()) {
        continue;
      }

      StatusCode.Code code = StatusCode.Code.valueOf(trimmed);
      Preconditions.checkNotNull(code, String.format("Unknown status code %s found", stringCode));
      statusCodeBuilder.add(code);
      LOG.debug("gRPC retry on: %s", stringCode);
    }
    return statusCodeBuilder.build();
  }

  private <Request, Response> void buildIdempotentCallSettings(
      UnaryCallSettings.Builder<Request, Response> unaryCallSettings) {

    ImmutableSet.Builder<StatusCode.Code> retryCodeBuilder = ImmutableSet.builder();
    if (configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, true)) {
      retryCodeBuilder
          .addAll(extractRetryCodesFromConfig())
          .addAll(unaryCallSettings.getRetryableCodes());
    }

    unaryCallSettings
        .setRetryableCodes(retryCodeBuilder.build())
        .setRetrySettings(buildIdempotentRetrySettings(unaryCallSettings.getRetrySettings()));
  }

  private void buildNonIdempotentCallSettings(UnaryCallSettings.Builder unaryCallSettings) {
    String shortRpcTimeoutStr = configuration.get(BIGTABLE_RPC_TIMEOUT_MS_KEY);
    if (!isNullOrEmpty(shortRpcTimeoutStr)) {
      unaryCallSettings.setSimpleTimeoutNoRetries(ofMillis(Long.parseLong(shortRpcTimeoutStr)));
    }
  }

  /** Creates {@link RetrySettings} for non-streaming VENEER_ADAPTER method. */
  private RetrySettings buildIdempotentRetrySettings(RetrySettings originalRetrySettings) {
    RetrySettings.Builder retryBuilder = originalRetrySettings.toBuilder();

    if (configuration.getBoolean(ALLOW_NO_TIMESTAMP_RETRIES_KEY, false)) {
      throw new UnsupportedOperationException("Retries without Timestamp is not supported yet.");
    }

    String initialElapsedBackoffMsStr = configuration.get(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY);
    if (!isNullOrEmpty(initialElapsedBackoffMsStr)) {
      retryBuilder.setInitialRetryDelay(ofMillis(Long.parseLong(initialElapsedBackoffMsStr)));
    }

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_TIMEOUTS_KEY))) {
      String shortRpcTimeoutMsStr = configuration.get(BIGTABLE_RPC_TIMEOUT_MS_KEY);

      if (!isNullOrEmpty(shortRpcTimeoutMsStr)) {
        Duration rpcTimeoutMs = ofMillis(Long.valueOf(shortRpcTimeoutMsStr));
        retryBuilder.setInitialRpcTimeout(rpcTimeoutMs).setMaxRpcTimeout(rpcTimeoutMs);
      }
    }

    String maxElapsedBackoffMillis = configuration.get(MAX_ELAPSED_BACKOFF_MILLIS_KEY);
    if (!isNullOrEmpty(maxElapsedBackoffMillis)) {
      retryBuilder.setTotalTimeout(ofMillis(Long.valueOf(maxElapsedBackoffMillis)));
    }

    return retryBuilder.build();
  }

  private void buildBulkMutationsSettings(EnhancedBigtableStubSettings.Builder builder) {
    BatchingSettings.Builder batchMutateBuilder =
        builder.bulkMutateRowsSettings().getBatchingSettings().toBuilder();

    String autoFlushStr = configuration.get(BIGTABLE_BULK_AUTOFLUSH_MS_KEY);
    if (!isNullOrEmpty(autoFlushStr)) {
      long autoFlushMs = Long.parseLong(autoFlushStr);
      if (autoFlushMs > 0) {
        batchMutateBuilder.setDelayThreshold(ofMillis(autoFlushMs));
      }
    }

    String bulkMaxRowKeyCountStr = configuration.get(BIGTABLE_BULK_MAX_ROW_KEY_COUNT);
    if (!isNullOrEmpty(bulkMaxRowKeyCountStr)) {
      batchMutateBuilder.setElementCountThreshold(Long.parseLong(bulkMaxRowKeyCountStr));
    }

    long bulkMaxRowKeyCount = batchMutateBuilder.build().getElementCountThreshold();

    String maxInflightRpcStr = configuration.get(MAX_INFLIGHT_RPCS_KEY);
    if (!isNullOrEmpty(maxInflightRpcStr) && Integer.parseInt(maxInflightRpcStr) > 0) {

      int maxInflightRpcCount = Integer.parseInt(maxInflightRpcStr);
      FlowControlSettings.Builder flowControlBuilder =
          FlowControlSettings.newBuilder()
              // TODO: verify if it should be channelCount instead of maxRowKeyCount
              .setMaxOutstandingElementCount(maxInflightRpcCount * bulkMaxRowKeyCount);

      String maxMemory = configuration.get(BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY);
      if (!isNullOrEmpty(maxMemory)) {
        flowControlBuilder.setMaxOutstandingRequestBytes(Long.valueOf(maxMemory));
      }

      batchMutateBuilder.setFlowControlSettings(flowControlBuilder.build());
    }

    String requestByteThresholdStr = configuration.get(BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES);
    if (!isNullOrEmpty(requestByteThresholdStr)) {
      batchMutateBuilder.setRequestByteThreshold(Long.valueOf(requestByteThresholdStr));
    }

    ImmutableSet.Builder<StatusCode.Code> retryCodes = ImmutableSet.builder();
    if (configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, true)) {
      retryCodes
          .addAll(extractRetryCodesFromConfig())
          .addAll(builder.bulkMutateRowsSettings().getRetryableCodes());
    }

    builder
        .bulkMutateRowsSettings()
        .setBatchingSettings(batchMutateBuilder.build())
        .setRetryableCodes(retryCodes.build())
        .setRetrySettings(
            buildIdempotentRetrySettings(builder.bulkMutateRowsSettings().getRetrySettings()));
  }

  private void buildBulkReadRowsSettings(EnhancedBigtableStubSettings.Builder builder) {
    BatchingSettings.Builder bulkReadBatchingBuilder =
        builder.bulkReadRowsSettings().getBatchingSettings().toBuilder();

    String bulkMaxRowKeyCountStr = configuration.get(BIGTABLE_BULK_MAX_ROW_KEY_COUNT);
    if (!isNullOrEmpty(bulkMaxRowKeyCountStr)) {
      bulkReadBatchingBuilder.setElementCountThreshold(Long.valueOf(bulkMaxRowKeyCountStr));
    }

    ImmutableSet.Builder<StatusCode.Code> retryCodes = ImmutableSet.builder();
    if (configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, true)) {
      retryCodes
          .addAll(extractRetryCodesFromConfig())
          .addAll(builder.bulkReadRowsSettings().getRetryableCodes());
    }

    builder
        .bulkReadRowsSettings()
        .setBatchingSettings(bulkReadBatchingBuilder.build())
        .setRetryableCodes(retryCodes.build())
        .setRetrySettings(
            buildIdempotentRetrySettings(builder.bulkReadRowsSettings().getRetrySettings()));
  }

  private void buildReadRowsSettings(EnhancedBigtableStubSettings.Builder stubSettings) {
    RetrySettings.Builder retryBuilder =
        stubSettings.readRowsSettings().getRetrySettings().toBuilder();

    String initialElapsedBackoffMsStr = configuration.get(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY);
    if (!isNullOrEmpty(initialElapsedBackoffMsStr)) {
      retryBuilder.setInitialRetryDelay(ofMillis(Long.valueOf(initialElapsedBackoffMsStr)));
    }

    String maxScanTimeoutRetriesAttempts = configuration.get(MAX_SCAN_TIMEOUT_RETRIES);
    if (!isNullOrEmpty(maxScanTimeoutRetriesAttempts)) {
      LOG.debug("gRPC max scan timeout retries (count): %d", maxScanTimeoutRetriesAttempts);
      retryBuilder.setMaxAttempts(Integer.valueOf(maxScanTimeoutRetriesAttempts));
    }

    String rpcTimeoutStr = configuration.get(READ_PARTIAL_ROW_TIMEOUT_MS);
    if (!isNullOrEmpty(rpcTimeoutStr)) {
      Duration rpcTimeoutMs = ofMillis(Long.valueOf(rpcTimeoutStr));
      retryBuilder.setInitialRpcTimeout(rpcTimeoutMs).setMaxRpcTimeout(rpcTimeoutMs);
    }

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_TIMEOUTS_KEY))) {
      String readRowsRpcTimeoutMs = configuration.get(BIGTABLE_READ_RPC_TIMEOUT_MS_KEY);

      if (!isNullOrEmpty(readRowsRpcTimeoutMs)) {
        retryBuilder.setTotalTimeout(ofMillis(Long.valueOf(readRowsRpcTimeoutMs)));
      }
    } else {

      String maxElapsedBackoffMillis = configuration.get(MAX_ELAPSED_BACKOFF_MILLIS_KEY);
      if (!isNullOrEmpty(maxElapsedBackoffMillis)) {
        retryBuilder.setTotalTimeout(ofMillis(Long.valueOf(maxElapsedBackoffMillis)));
      }
    }

    ImmutableSet.Builder<StatusCode.Code> retryCodes = ImmutableSet.builder();
    if (configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, true)) {
      retryCodes
          .addAll(extractRetryCodesFromConfig())
          .addAll(stubSettings.readRowsSettings().getRetryableCodes());
    }

    stubSettings
        .readRowsSettings()
        .setRetryableCodes(retryCodes.build())
        .setRetrySettings(retryBuilder.build());
  }

  private void buildEmulatorSettings(StubSettings.Builder stubSettings) {
    String emulatorHostPort = configuration.get(BIGTABLE_EMULATOR_HOST_KEY);
    if (!isNullOrEmpty(emulatorHostPort)) {
      stubSettings
          .setCredentialsProvider(NoCredentialsProvider.create())
          .setEndpoint(emulatorHostPort)
          .setTransportChannelProvider(buildPlainTextChannelProvider(emulatorHostPort));
    }
  }
}
