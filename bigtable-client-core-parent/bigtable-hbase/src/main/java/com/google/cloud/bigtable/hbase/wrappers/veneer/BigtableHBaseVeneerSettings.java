/*
 * Copyright 2020 Google LLC
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
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_AUTOFLUSH_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_PORT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_BATCH;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ENABLE_GRPC_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_INFLIGHT_RPCS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_PARTIAL_ROW_TIMEOUT_MS;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.grpc.internal.GrpcUtil.USER_AGENT_KEY;
import static org.threeten.bp.Duration.ofMillis;
import static org.threeten.bp.Duration.ofMinutes;
import static org.threeten.bp.Duration.ofSeconds;

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
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StubSettings;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.stub.BigtableInstanceAdminStubSettings;
import com.google.cloud.bigtable.admin.v2.stub.BigtableTableAdminStubSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.cloud.bigtable.hbase.BigtableExtendedConfiguration;
import com.google.cloud.bigtable.hbase.BigtableHBaseVersion;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.veneer.metrics.MetricsApiTracerAdapterFactory;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.grpc.ManagedChannelBuilder;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.threeten.bp.Duration;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableHBaseVeneerSettings extends BigtableHBaseSettings {

  private static final Duration EFFECTIVELY_DISABLED_DEADLINE_DURATION = Duration.ofHours(12);
  private static final String BIGTABLE_BATCH_DATA_HOST_DEFAULT = "batch-bigtable.googleapis.com";
  private static final Duration INITIAL_RETRY_IN_BATCH_MODE = ofSeconds(5);
  private static final Duration MAX_ELAPSED_BACKOFF_IN_BATCH_MODE = ofMinutes(5);

  private final Configuration configuration;
  private final BigtableDataSettings dataSettings;
  private final BigtableTableAdminSettings tableAdminSettings;
  @Nullable private final BigtableInstanceAdminSettings instanceAdminSettings;

  private final String dataHost;
  private final String adminHost;
  private final int port;
  private final int bulkMaxRowKeyCount;
  private final long batchingMaxMemory;
  private final boolean isChannelPoolCachingEnabled;
  private final boolean allowRetriesWithoutTimestamp;

  public BigtableHBaseVeneerSettings(Configuration configuration) throws IOException {
    super(configuration);
    // we can't create a defensive copy because it might be an instance
    // BigtableExtendedConfiguration
    this.configuration = configuration;
    this.dataSettings = buildBigtableDataSettings();
    this.tableAdminSettings = buildBigtableTableAdminSettings();

    if (!isNullOrEmpty(configuration.get(BIGTABLE_EMULATOR_HOST_KEY))) {
      this.instanceAdminSettings = null;
    } else {
      this.instanceAdminSettings = buildBigtableInstanceAdminSettings();
    }

    String dataEndpoint = dataSettings.getStubSettings().getEndpoint();
    this.dataHost = dataEndpoint.substring(0, dataEndpoint.lastIndexOf(":"));

    String adminEndpoint = tableAdminSettings.getStubSettings().getEndpoint();
    this.adminHost = adminEndpoint.substring(0, adminEndpoint.lastIndexOf(":"));

    this.port = Integer.parseInt(dataEndpoint.substring(dataEndpoint.lastIndexOf(":") + 1));

    this.bulkMaxRowKeyCount =
        dataSettings
            .getStubSettings()
            .bulkMutateRowsSettings()
            .getBatchingSettings()
            .getElementCountThreshold()
            .intValue();

    this.batchingMaxMemory =
        dataSettings
            .getStubSettings()
            .bulkMutateRowsSettings()
            .getBatchingSettings()
            .getFlowControlSettings()
            .getMaxOutstandingRequestBytes();

    // This is primarily used by Dataflow where connections open and close often. This is a
    // performance optimization that will reduce the cost to open connections.
    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL))
        || Boolean.parseBoolean(configuration.get(BIGTABLE_USE_BATCH))) {
      this.isChannelPoolCachingEnabled = true;
    } else {
      this.isChannelPoolCachingEnabled = false;
    }

    this.allowRetriesWithoutTimestamp =
        Boolean.parseBoolean(configuration.get(ALLOW_NO_TIMESTAMP_RETRIES_KEY));
  }

  @Override
  public String getDataHost() {
    return dataHost;
  }

  @Override
  public String getAdminHost() {
    return adminHost;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public int getBulkMaxRowCount() {
    return bulkMaxRowKeyCount;
  }

  @Override
  public long getBatchingMaxRequestSize() {
    return batchingMaxMemory;
  }

  @Override
  public boolean isRetriesWithoutTimestampAllowed() {
    return allowRetriesWithoutTimestamp;
  }

  // ************** Getters **************
  public boolean isChannelPoolCachingEnabled() {
    return isChannelPoolCachingEnabled;
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

    configureConnection(stubSettings, BIGTABLE_HOST_KEY);

    configureHeaderProvider(stubSettings);

    configureCredentialProvider(stubSettings);

    configureEmulatorSettings(stubSettings);

    // RPC methods
    configureBulkMutationSettings(stubSettings);

    configureBulkReadRowsSettings(stubSettings);

    configureMutateRowSettings(stubSettings);

    configureReadRowsSettings(stubSettings);

    configureNonIdempotentCallSettings(stubSettings.checkAndMutateRowSettings());
    configureNonIdempotentCallSettings(stubSettings.readModifyWriteRowSettings());

    configureIdempotentCallSettings(stubSettings.readRowSettings());
    configureIdempotentCallSettings(stubSettings.sampleRowKeysSettings());

    configureMetricsBridge(dataBuilder);

    return dataBuilder.build();
  }

  private void configureMetricsBridge(Builder settings) {
    MetricsApiTracerAdapterFactory metricsApiTracerAdapterFactory =
        new MetricsApiTracerAdapterFactory();
    settings.stubSettings().setTracerFactory(metricsApiTracerAdapterFactory);
  }

  private BigtableTableAdminSettings buildBigtableTableAdminSettings() throws IOException {
    BigtableTableAdminSettings.Builder adminBuilder =
        BigtableTableAdminSettings.newBuilder()
            .setProjectId(getProjectId())
            .setInstanceId(getInstanceId());

    BigtableTableAdminStubSettings.Builder stubSettings = adminBuilder.stubSettings();

    configureConnection(stubSettings, BIGTABLE_ADMIN_HOST_KEY);

    configureHeaderProvider(stubSettings);

    configureCredentialProvider(stubSettings);

    configureEmulatorSettings(stubSettings);

    return adminBuilder.build();
  }

  private BigtableInstanceAdminSettings buildBigtableInstanceAdminSettings() throws IOException {
    BigtableInstanceAdminSettings.Builder instanceAdminBuilder =
        BigtableInstanceAdminSettings.newBuilder().setProjectId(getProjectId());

    BigtableInstanceAdminStubSettings.Builder stubSettings = instanceAdminBuilder.stubSettings();

    configureConnection(stubSettings, BIGTABLE_ADMIN_HOST_KEY);

    configureHeaderProvider(stubSettings);

    configureCredentialProvider(stubSettings);

    return instanceAdminBuilder.build();
  }

  private void configureConnection(StubSettings.Builder stubSettings, String endpointKey) {

    String endpoint = stubSettings.getEndpoint();
    String portNumber = configuration.get(BIGTABLE_PORT_KEY);
    if (isNullOrEmpty(portNumber)) {
      portNumber = endpoint.substring(endpoint.lastIndexOf(":") + 1);
    }

    String hostOverride = configuration.get(endpointKey);
    if (isNullOrEmpty(hostOverride)) {
      hostOverride = endpoint.substring(0, endpoint.lastIndexOf(":"));
      LOG.debug("%s is configured at %s", endpointKey, hostOverride);
    }

    if (isBatchModeEnabled() && BIGTABLE_HOST_KEY.equals(endpointKey)) {
      // TODO: move this constant in default alignment PR.
      hostOverride = BIGTABLE_BATCH_DATA_HOST_DEFAULT;
    }
    stubSettings.setEndpoint(hostOverride + ":" + portNumber);

    InstantiatingGrpcChannelProvider.Builder channelBuilder = defaultGrpcTransportProviderBuilder();

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_PLAINTEXT_NEGOTIATION))) {
      channelBuilder.setChannelConfigurator(
          new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
            @Override
            public ManagedChannelBuilder apply(ManagedChannelBuilder channelBuilder) {
              return channelBuilder.usePlaintext();
            }
          });
    }

    String channelCount = configuration.get(BIGTABLE_DATA_CHANNEL_COUNT_KEY);
    if (!isNullOrEmpty(channelCount)) {
      channelBuilder.setPoolSize(Integer.parseInt(channelCount));
    }

    stubSettings.setTransportChannelProvider(channelBuilder.build());
  }

  private void configureHeaderProvider(StubSettings.Builder stubSettings) {

    StringBuilder agentBuilder = new StringBuilder();
    agentBuilder
        .append("hbase-")
        .append(VersionInfo.getVersion())
        .append(",")
        .append("java-bigtable-hbase-")
        .append(BigtableHBaseVersion.getVersion());

    String customUserAgent = configuration.get(CUSTOM_USER_AGENT_KEY);
    if (customUserAgent != null) {
      agentBuilder.append(',').append(customUserAgent);
    }

    stubSettings.setHeaderProvider(
        FixedHeaderProvider.create(USER_AGENT_KEY.name(), agentBuilder.toString()));
  }

  private void configureCredentialProvider(StubSettings.Builder stubSettings) throws IOException {

    // This preserves user defined Credentials
    if (configuration instanceof BigtableExtendedConfiguration) {
      Credentials credentials = ((BigtableExtendedConfiguration) configuration).getCredentials();
      stubSettings.setCredentialsProvider(FixedCredentialsProvider.create(credentials));

    } else if (Boolean.parseBoolean(configuration.get(BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY))) {
      stubSettings.setCredentialsProvider(NoCredentialsProvider.create());
      LOG.info("Enabling the use of null credentials. This should not be used in production.");

    } else if (!isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY))) {
      String jsonValue = configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY);
      LOG.debug("Using json value");
      stubSettings.setCredentialsProvider(
          FixedCredentialsProvider.create(
              GoogleCredentials.fromStream(
                  new ByteArrayInputStream(jsonValue.getBytes(StandardCharsets.UTF_8)))));

    } else if (!isNullOrEmpty(
        configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY))) {
      String keyFileLocation =
          configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY);
      LOG.debug("Using json keyfile: %s", keyFileLocation);
      stubSettings.setCredentialsProvider(
          FixedCredentialsProvider.create(
              GoogleCredentials.fromStream(new FileInputStream(keyFileLocation))));

    } else if (!isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY))) {
      String serviceAccount = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
      LOG.debug("Service account %s specified.", serviceAccount);
      String keyFileLocation = configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
      Preconditions.checkState(
          !isNullOrEmpty(keyFileLocation),
          "Key file location must be specified when setting service account email");
      LOG.debug("Using p12 keyfile: %s", keyFileLocation);
      stubSettings.setCredentialsProvider(
          FixedCredentialsProvider.create(
              buildCredentialFromPrivateKey(serviceAccount, keyFileLocation)));
    }
  }

  private Credentials buildCredentialFromPrivateKey(
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

  private void configureEmulatorSettings(StubSettings.Builder stubSettings) {
    String emulatorHostPort = configuration.get(BIGTABLE_EMULATOR_HOST_KEY);
    if (!isNullOrEmpty(emulatorHostPort)) {
      stubSettings
          .setCredentialsProvider(NoCredentialsProvider.create())
          .setEndpoint(emulatorHostPort)
          .setTransportChannelProvider(
              InstantiatingGrpcChannelProvider.newBuilder()
                  .setEndpoint(emulatorHostPort)
                  .setPoolSize(1)
                  .setChannelConfigurator(
                      new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
                        @Override
                        public ManagedChannelBuilder apply(ManagedChannelBuilder channelBuilder) {
                          return channelBuilder.usePlaintext();
                        }
                      })
                  .build());
    }
  }

  private void configureBulkMutationSettings(EnhancedBigtableStubSettings.Builder builder) {

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING))) {
      throw new UnsupportedOperationException("Buffered mutator throttling is not supported.");
    }

    BatchingSettings.Builder batchingSettingsBuilder =
        builder.bulkMutateRowsSettings().getBatchingSettings().toBuilder();

    String autoFlushStr = configuration.get(BIGTABLE_BULK_AUTOFLUSH_MS_KEY);
    if (!isNullOrEmpty(autoFlushStr)) {
      long autoFlushMs = Long.parseLong(autoFlushStr);
      if (autoFlushMs > 0) {
        batchingSettingsBuilder.setDelayThreshold(ofMillis(autoFlushMs));
      }
    }

    String bulkMaxRowKeyCountStr = configuration.get(BIGTABLE_BULK_MAX_ROW_KEY_COUNT);
    if (!isNullOrEmpty(bulkMaxRowKeyCountStr)) {
      batchingSettingsBuilder.setElementCountThreshold(Long.parseLong(bulkMaxRowKeyCountStr));
    }

    String maxInflightRpcStr = configuration.get(MAX_INFLIGHT_RPCS_KEY);
    if (!isNullOrEmpty(maxInflightRpcStr) && Integer.parseInt(maxInflightRpcStr) > 0) {

      int maxInflightRpcCount = Integer.parseInt(maxInflightRpcStr);

      // This needs to be extracted for calculating outstandingElementCount
      long bulkMaxRowKeyCount =
          builder.bulkMutateRowsSettings().getBatchingSettings().getElementCountThreshold();

      FlowControlSettings.Builder flowControlBuilder =
          FlowControlSettings.newBuilder()
              // TODO: either deprecate maxInflightRpcCount and expose the max outstanding elements
              // in user configuration or introduce maxInflightRpcCount to gax
              .setMaxOutstandingElementCount(maxInflightRpcCount * bulkMaxRowKeyCount);

      String maxMemory = configuration.get(BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY);
      if (!isNullOrEmpty(maxMemory)) {
        flowControlBuilder.setMaxOutstandingRequestBytes(Long.valueOf(maxMemory));
      }

      batchingSettingsBuilder.setFlowControlSettings(flowControlBuilder.build());
    }

    String requestByteThresholdStr = configuration.get(BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES);
    if (!isNullOrEmpty(requestByteThresholdStr)) {
      batchingSettingsBuilder.setRequestByteThreshold(Long.valueOf(requestByteThresholdStr));
    }

    builder.bulkMutateRowsSettings().setBatchingSettings(batchingSettingsBuilder.build());

    configureIdempotentCallSettings(builder.bulkMutateRowsSettings());
  }

  private void configureBulkReadRowsSettings(EnhancedBigtableStubSettings.Builder builder) {
    BatchingSettings.Builder bulkReadBatchingBuilder =
        builder.bulkReadRowsSettings().getBatchingSettings().toBuilder();

    String bulkMaxRowKeyCountStr = configuration.get(BIGTABLE_BULK_MAX_ROW_KEY_COUNT);
    if (!isNullOrEmpty(bulkMaxRowKeyCountStr)) {
      bulkReadBatchingBuilder.setElementCountThreshold(Long.valueOf(bulkMaxRowKeyCountStr));
    }

    builder.bulkReadRowsSettings().setBatchingSettings(bulkReadBatchingBuilder.build());

    configureIdempotentCallSettings(builder.bulkReadRowsSettings());
  }

  private void configureMutateRowSettings(EnhancedBigtableStubSettings.Builder stubSettings) {
    UnaryCallSettings.Builder mutateRowSettingsBuilder = stubSettings.mutateRowSettings();

    configureIdempotentCallSettings(mutateRowSettingsBuilder);

    RetrySettings.Builder retryBuilder = mutateRowSettingsBuilder.getRetrySettings().toBuilder();

    String mutateRpcTimeoutMs = configuration.get(BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY);
    if (!isNullOrEmpty(mutateRpcTimeoutMs)) {

      retryBuilder.setTotalTimeout(ofMillis(Long.parseLong(mutateRpcTimeoutMs)));
    } else if (!isNullOrEmpty(configuration.get(BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY))) {

      long longRpcTimeoutMs = Long.parseLong(configuration.get(BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY));
      retryBuilder.setTotalTimeout(ofMillis(longRpcTimeoutMs));
    }
    mutateRowSettingsBuilder.setRetrySettings(retryBuilder.build());
  }

  private void configureReadRowsSettings(EnhancedBigtableStubSettings.Builder stubSettings) {
    RetrySettings.Builder retryBuilder =
        stubSettings.readRowsSettings().getRetrySettings().toBuilder();

    if (isRetriesDisabled()) {

      stubSettings.readRowsSettings().setRetryableCodes(Collections.<StatusCode.Code>emptySet());
    } else {
      ImmutableSet.Builder<StatusCode.Code> retryCodes = ImmutableSet.builder();

      retryCodes
          .addAll(extractRetryCodesFromConfig())
          .addAll(stubSettings.readRowsSettings().getRetryableCodes());

      stubSettings.readRowsSettings().setRetryableCodes(retryCodes.build());

      String initialElapsedBackoffMsStr = configuration.get(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY);
      if (!isNullOrEmpty(initialElapsedBackoffMsStr)) {

        long initialElapsedBackoffMs = Long.parseLong(initialElapsedBackoffMsStr);
        retryBuilder.setInitialRetryDelay(ofMillis(initialElapsedBackoffMs));

        if (initialElapsedBackoffMs > retryBuilder.getMaxRetryDelay().toMillis()) {
          // TODO: fix this scenario by maybe introducing maxRetryDelayMillis directly
          retryBuilder.setMaxRetryDelay(ofMillis(initialElapsedBackoffMs));
        }
      } else if (isBatchModeEnabled()) {
        // TODO: move this constant in default alignment PR.
        retryBuilder.setInitialRetryDelay(INITIAL_RETRY_IN_BATCH_MODE);
      }

      String maxElapsedBackoffMillis = configuration.get(MAX_ELAPSED_BACKOFF_MILLIS_KEY);
      if (!isNullOrEmpty(maxElapsedBackoffMillis)) {
        retryBuilder.setTotalTimeout(ofMillis(Long.parseLong(maxElapsedBackoffMillis)));
      } else if (isBatchModeEnabled()) {
        // TODO: move this constant in default alignment PR.
        retryBuilder.setTotalTimeout(MAX_ELAPSED_BACKOFF_IN_BATCH_MODE);
      }

      String maxScanTimeoutRetriesAttemptsStr = configuration.get(MAX_SCAN_TIMEOUT_RETRIES);
      if (!isNullOrEmpty(maxScanTimeoutRetriesAttemptsStr)) {
        int maxScanTimeoutRetriesAttempts = Integer.parseInt(maxScanTimeoutRetriesAttemptsStr);
        LOG.debug("gRPC max scan timeout retries (count): %d", maxScanTimeoutRetriesAttempts);
        retryBuilder.setMaxAttempts(maxScanTimeoutRetriesAttempts);
      }
    }

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_TIMEOUTS_KEY))) {
      String readRowsRpcTimeoutMs = configuration.get(BIGTABLE_READ_RPC_TIMEOUT_MS_KEY);

      if (!isNullOrEmpty(readRowsRpcTimeoutMs)) {
        retryBuilder.setTotalTimeout(ofMillis(Long.parseLong(readRowsRpcTimeoutMs)));
      } else if (!isNullOrEmpty(configuration.get(BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY))) {

        long longRpcTimeoutMs = Long.parseLong(configuration.get(BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY));
        retryBuilder.setTotalTimeout(ofMillis(longRpcTimeoutMs));
      }

      String rpcTimeoutStr = configuration.get(READ_PARTIAL_ROW_TIMEOUT_MS);
      if (!isNullOrEmpty(rpcTimeoutStr)) {
        Duration rpcTimeoutMs = ofMillis(Long.parseLong(rpcTimeoutStr));
        retryBuilder.setInitialRpcTimeout(rpcTimeoutMs).setMaxRpcTimeout(rpcTimeoutMs);
      }
    } else {

      retryBuilder
          .setInitialRpcTimeout(EFFECTIVELY_DISABLED_DEADLINE_DURATION)
          .setMaxRpcTimeout(EFFECTIVELY_DISABLED_DEADLINE_DURATION)
          .setTotalTimeout(EFFECTIVELY_DISABLED_DEADLINE_DURATION);
    }

    stubSettings.readRowsSettings().setRetrySettings(retryBuilder.build());
  }

  private void configureIdempotentCallSettings(UnaryCallSettings.Builder unaryCallSettings) {

    RetrySettings.Builder retryBuilder = unaryCallSettings.getRetrySettings().toBuilder();
    Set<StatusCode.Code> retryCodeBuilder = new HashSet<>();

    if (isRetriesDisabled()) {

      unaryCallSettings.setRetryableCodes(Collections.<StatusCode.Code>emptySet());
    } else {

      retryCodeBuilder.addAll(extractRetryCodesFromConfig());
      retryCodeBuilder.addAll(unaryCallSettings.getRetryableCodes());

      String enableDealLineRetry = configuration.get(ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY);
      if (!isNullOrEmpty(enableDealLineRetry)) {

        if ("true".equalsIgnoreCase(enableDealLineRetry)) {
          retryCodeBuilder.add(StatusCode.Code.DEADLINE_EXCEEDED);
        } else if ("false".equalsIgnoreCase(enableDealLineRetry)) {
          retryCodeBuilder.remove(StatusCode.Code.DEADLINE_EXCEEDED);
        }
      }
      unaryCallSettings.setRetryableCodes(Collections.unmodifiableSet(retryCodeBuilder));

      String initialElapsedBackoffMsStr = configuration.get(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY);
      if (!isNullOrEmpty(initialElapsedBackoffMsStr)) {

        long initialElapsedBackoffMs = Long.parseLong(initialElapsedBackoffMsStr);
        retryBuilder.setInitialRetryDelay(ofMillis(initialElapsedBackoffMs));

        if (initialElapsedBackoffMs > retryBuilder.getMaxRetryDelay().toMillis()) {
          // TODO: fix this scenario by maybe introducing maxRetryDelayMillis directly
          retryBuilder.setMaxRetryDelay(ofMillis(initialElapsedBackoffMs));
        }
      } else if (isBatchModeEnabled()) {
        // TODO: move this constant in default alignment PR.
        retryBuilder.setInitialRetryDelay(INITIAL_RETRY_IN_BATCH_MODE);
      }
    }

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_USE_TIMEOUTS_KEY))) {

      String shortRpcTimeoutMsStr = configuration.get(BIGTABLE_RPC_TIMEOUT_MS_KEY);
      if (!isNullOrEmpty(shortRpcTimeoutMsStr)) {
        Duration rpcTimeoutMs = ofMillis(Long.parseLong(shortRpcTimeoutMsStr));
        retryBuilder.setInitialRpcTimeout(rpcTimeoutMs).setMaxRpcTimeout(rpcTimeoutMs);
      }

      String maxElapsedBackoffMsStr = configuration.get(MAX_ELAPSED_BACKOFF_MILLIS_KEY);
      if (!isNullOrEmpty(maxElapsedBackoffMsStr)) {
        retryBuilder.setTotalTimeout(ofMillis(Long.parseLong(maxElapsedBackoffMsStr)));
      } else if (isBatchModeEnabled()) {
        // TODO: move this constant in default alignment PR.
        retryBuilder.setTotalTimeout(MAX_ELAPSED_BACKOFF_IN_BATCH_MODE);
      }
    } else {

      retryBuilder
          .setInitialRpcTimeout(EFFECTIVELY_DISABLED_DEADLINE_DURATION)
          .setMaxRpcTimeout(EFFECTIVELY_DISABLED_DEADLINE_DURATION)
          .setTotalTimeout(EFFECTIVELY_DISABLED_DEADLINE_DURATION);
    }

    unaryCallSettings.setRetrySettings(retryBuilder.build());
  }

  private void configureNonIdempotentCallSettings(UnaryCallSettings.Builder unaryCallSettings) {
    String shortRpcTimeoutStr = configuration.get(BIGTABLE_RPC_TIMEOUT_MS_KEY);
    if (!isNullOrEmpty(shortRpcTimeoutStr)) {
      unaryCallSettings.setSimpleTimeoutNoRetries(ofMillis(Long.parseLong(shortRpcTimeoutStr)));
    }
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

  private boolean isBatchModeEnabled() {
    return configuration.getBoolean(BIGTABLE_USE_BATCH, false);
  }

  private boolean isRetriesDisabled() {
    return !configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, true);
  }
}
