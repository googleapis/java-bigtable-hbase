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

import static com.google.cloud.bigtable.config.BulkOptions.BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ADDITIONAL_RETRY_CODES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ALLOW_NO_TIMESTAMP_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.APP_PROFILE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_AUTOFLUSH_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_ATTEMPT_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_PORT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_ATTEMPT_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_BATCH;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ENABLE_GRPC_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_INFLIGHT_RPCS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_PARTIAL_ROW_TIMEOUT_MS;
import static io.grpc.internal.GrpcUtil.USER_AGENT_KEY;

import com.google.api.core.ApiFunction;
import com.google.api.core.InternalApi;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.ServerStreamingCallSettings;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StubSettings;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.stub.BigtableInstanceAdminStubSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings.Builder;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.stub.BigtableBatchingCallSettings;
import com.google.cloud.bigtable.data.v2.stub.BigtableBulkReadRowsCallSettings;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableExtendedConfiguration;
import com.google.cloud.bigtable.hbase.BigtableHBaseVersion;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.veneer.metrics.MetricsApiTracerAdapterFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.grpc.ManagedChannelBuilder;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.threeten.bp.Duration;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableHBaseVeneerSettings extends BigtableHBaseSettings {
  private static final String BIGTABLE_BATCH_DATA_HOST_DEFAULT = "batch-bigtable.googleapis.com";

  private final Configuration configuration;
  private final BigtableDataSettings dataSettings;
  private final BigtableTableAdminSettings tableAdminSettings;
  private final BigtableInstanceAdminSettings instanceAdminSettings;

  private final String dataHost;
  private final String adminHost;
  private final int port;
  private final int bulkMaxRowKeyCount;
  private final long batchingMaxMemory;
  private final boolean isChannelPoolCachingEnabled;
  private final boolean allowRetriesWithoutTimestamp;

  public static BigtableHBaseVeneerSettings create(Configuration configuration) throws IOException {
    Configuration copy = new Configuration(configuration);

    if (configuration instanceof BigtableExtendedConfiguration) {
      BigtableExtendedConfiguration extConfig = (BigtableExtendedConfiguration) configuration;
      copy = BigtableConfiguration.withCredentials(copy, extConfig.getCredentials());
    }
    return new BigtableHBaseVeneerSettings(copy);
  }

  private BigtableHBaseVeneerSettings(Configuration configuration) throws IOException {
    super(configuration);
    this.configuration = configuration;

    // Build configs for veneer
    this.dataSettings = buildBigtableDataSettings();
    this.tableAdminSettings = buildBigtableTableAdminSettings();
    this.instanceAdminSettings = buildBigtableInstanceAdminSettings();

    // Veneer settings are finalized, now we need to extract java-bigtable-hbase
    // specific settings
    String dataEndpoint = dataSettings.getStubSettings().getEndpoint();
    this.dataHost = dataEndpoint.substring(0, dataEndpoint.lastIndexOf(":"));

    String adminEndpoint = tableAdminSettings.getStubSettings().getEndpoint();
    this.adminHost = adminEndpoint.substring(0, adminEndpoint.lastIndexOf(":"));

    this.port = Integer.parseInt(dataEndpoint.substring(dataEndpoint.lastIndexOf(":") + 1));

    //noinspection ConstantConditions
    this.bulkMaxRowKeyCount =
        dataSettings
            .getStubSettings()
            .bulkMutateRowsSettings()
            .getBatchingSettings()
            .getElementCountThreshold()
            .intValue();

    this.batchingMaxMemory =
        Objects.requireNonNull(
            dataSettings
                .getStubSettings()
                .bulkMutateRowsSettings()
                .getBatchingSettings()
                .getFlowControlSettings()
                .getMaxOutstandingRequestBytes());

    boolean batchingModeEnabled = configuration.getBoolean(BIGTABLE_USE_BATCH, false);
    this.isChannelPoolCachingEnabled =
        configuration.getBoolean(BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, batchingModeEnabled);

    this.allowRetriesWithoutTimestamp =
        configuration.getBoolean(ALLOW_NO_TIMESTAMP_RETRIES_KEY, false);
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
  public BigtableInstanceAdminSettings getInstanceAdminSettings() {
    return instanceAdminSettings;
  }

  // ************** Private Helpers **************
  private BigtableDataSettings buildBigtableDataSettings() throws IOException {
    BigtableDataSettings.Builder dataBuilder;

    // Configure the Data connection
    Optional<String> emulatorEndpoint =
        Optional.fromNullable(configuration.get(BIGTABLE_EMULATOR_HOST_KEY));
    if (emulatorEndpoint.isPresent()) {
      int split = emulatorEndpoint.get().lastIndexOf(':');
      String host = emulatorEndpoint.get().substring(0, split);
      int port = Integer.parseInt(emulatorEndpoint.get().substring(split + 1));
      dataBuilder = BigtableDataSettings.newBuilderForEmulator(host, port);
    } else {
      dataBuilder = BigtableDataSettings.newBuilder();
      configureConnection(dataBuilder.stubSettings(), BIGTABLE_HOST_KEY);
      configureCredentialProvider(dataBuilder.stubSettings());
    }
    configureHeaderProvider(dataBuilder.stubSettings());

    // Configure the target
    dataBuilder.setProjectId(getProjectId()).setInstanceId(getInstanceId());

    String appProfileId = configuration.get(APP_PROFILE_ID_KEY);
    if (!Strings.isNullOrEmpty(appProfileId)) {
      dataBuilder.setAppProfileId(appProfileId);
    }

    // Configure metrics
    configureMetricsBridge(dataBuilder);

    // Complex RPC method settings
    Optional<Duration> bulkMutateAttemptTimeout =
        extractDuration(BIGTABLE_MUTATE_RPC_ATTEMPT_TIMEOUT_MS_KEY);
    @SuppressWarnings("deprecation")
    Optional<Duration> bulkMutateTimeout =
        extractDuration(
            BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY,
            BigtableOptionsFactory.BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY);

    configureBulkMutationSettings(
        dataBuilder.stubSettings().bulkMutateRowsSettings(),
        bulkMutateAttemptTimeout,
        bulkMutateTimeout);

    Optional<Duration> readRowsAttemptTimeout =
        extractDuration(BIGTABLE_READ_RPC_ATTEMPT_TIMEOUT_MS_KEY);
    // NOTE: MAX_ELAPSED_BACKOFF_MILLIS_KEY is not used for scans
    @SuppressWarnings("deprecation")
    Optional<Duration> scanTimeout =
        extractDuration(
            BIGTABLE_READ_RPC_TIMEOUT_MS_KEY,
            BigtableOptionsFactory.BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY);

    configureBulkReadRowsSettings(
        dataBuilder.stubSettings().bulkReadRowsSettings(), readRowsAttemptTimeout, scanTimeout);
    configureReadRowsSettings(
        dataBuilder.stubSettings().readRowsSettings(), readRowsAttemptTimeout, scanTimeout);

    // RPC methods - simple
    Optional<Duration> shortAttemptTimeout = extractDuration(BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY);
    Optional<Duration> shortTimeout =
        extractDuration(BIGTABLE_RPC_TIMEOUT_MS_KEY, MAX_ELAPSED_BACKOFF_MILLIS_KEY);

    configureNonRetryableCallSettings(
        dataBuilder.stubSettings().checkAndMutateRowSettings(), shortTimeout);
    configureNonRetryableCallSettings(
        dataBuilder.stubSettings().readModifyWriteRowSettings(), shortTimeout);

    configureRetryableCallSettings(
        dataBuilder.stubSettings().mutateRowSettings(), shortAttemptTimeout, shortTimeout);
    configureRetryableCallSettings(
        dataBuilder.stubSettings().readRowSettings(), shortAttemptTimeout, shortTimeout);
    configureRetryableCallSettings(
        dataBuilder.stubSettings().sampleRowKeysSettings(), shortAttemptTimeout, shortTimeout);

    return dataBuilder.build();
  }

  private void configureMetricsBridge(Builder settings) {
    MetricsApiTracerAdapterFactory metricsApiTracerAdapterFactory =
        new MetricsApiTracerAdapterFactory();
    settings.stubSettings().setTracerFactory(metricsApiTracerAdapterFactory);
  }

  private BigtableTableAdminSettings buildBigtableTableAdminSettings() throws IOException {
    BigtableTableAdminSettings.Builder adminBuilder;

    // Configure connection
    String emulatorEndpoint = configuration.get(BIGTABLE_EMULATOR_HOST_KEY);
    if (!Strings.isNullOrEmpty(emulatorEndpoint)) {
      int split = emulatorEndpoint.lastIndexOf(':');
      String host = emulatorEndpoint.substring(0, split);
      int port = Integer.parseInt(emulatorEndpoint.substring(split + 1));
      adminBuilder = BigtableTableAdminSettings.newBuilderForEmulator(host, port);
    } else {
      adminBuilder = BigtableTableAdminSettings.newBuilder();
      configureConnection(adminBuilder.stubSettings(), BIGTABLE_ADMIN_HOST_KEY);
      configureCredentialProvider(adminBuilder.stubSettings());
    }
    configureHeaderProvider(adminBuilder.stubSettings());

    adminBuilder.setProjectId(getProjectId()).setInstanceId(getInstanceId());

    // timeout/retry settings don't apply to admin operations
    // v1 used to use RetryOptions for:
    // - createTable
    // - getTable
    // - listTables
    // - deleteTable
    // - modifyColumnFamilies
    // - dropRowRange
    // However data latencies are very different from data latencies and end users shouldn't need to
    // change the defaults
    // if it turns out that the timeout & retry behavior needs to be configurable, we will expose
    // separate settings

    return adminBuilder.build();
  }

  private BigtableInstanceAdminSettings buildBigtableInstanceAdminSettings() throws IOException {
    BigtableInstanceAdminSettings.Builder adminBuilder;

    // Configure connection
    String emulatorEndpoint = configuration.get(BIGTABLE_EMULATOR_HOST_KEY);
    if (!Strings.isNullOrEmpty(emulatorEndpoint)) {
      // todo switch to newBuilderForEmulator once available
      adminBuilder = BigtableInstanceAdminSettings.newBuilder();
      adminBuilder
          .stubSettings()
          .setEndpoint(emulatorEndpoint)
          .setCredentialsProvider(NoCredentialsProvider.create())
          .setTransportChannelProvider(
              BigtableInstanceAdminStubSettings.defaultGrpcTransportProviderBuilder()
                  .setChannelConfigurator(
                      new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
                        @Override
                        public ManagedChannelBuilder apply(
                            ManagedChannelBuilder managedChannelBuilder) {
                          return managedChannelBuilder.usePlaintext();
                        }
                      })
                  .build());
    } else {
      adminBuilder = BigtableInstanceAdminSettings.newBuilder();
      configureConnection(adminBuilder.stubSettings(), BIGTABLE_ADMIN_HOST_KEY);
      configureCredentialProvider(adminBuilder.stubSettings());
    }
    configureHeaderProvider(adminBuilder.stubSettings());

    adminBuilder.setProjectId(getProjectId());

    return adminBuilder.build();
  }

  private void configureConnection(StubSettings.Builder<?, ?> stubSettings, String endpointKey) {
    String defaultEndpoint = stubSettings.getEndpoint();
    String defaultHostname = defaultEndpoint.substring(0, defaultEndpoint.lastIndexOf(':'));
    String defaultPort = defaultEndpoint.substring(defaultEndpoint.lastIndexOf(':') + 1);

    Optional<String> hostOverride = Optional.fromNullable(configuration.get(endpointKey));
    Optional<String> portOverride = Optional.fromNullable(configuration.get(BIGTABLE_PORT_KEY));
    Optional<String> endpointOverride = Optional.absent();

    if (hostOverride.isPresent() || portOverride.isPresent()) {
      endpointOverride =
          Optional.of(hostOverride.or(defaultHostname) + ":" + portOverride.or(defaultPort));
    } else if (endpointKey.equals(BIGTABLE_HOST_KEY)
        && configuration.getBoolean(BIGTABLE_USE_BATCH, false)) {
      endpointOverride = Optional.of(BIGTABLE_BATCH_DATA_HOST_DEFAULT + ":443");
    }

    if (endpointOverride.isPresent()) {
      stubSettings.setEndpoint(endpointOverride.get());
      LOG.debug("%s is configured at %s", endpointKey, endpointOverride);
    }

    final InstantiatingGrpcChannelProvider.Builder channelProvider =
        ((InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider()).toBuilder();

    if (configuration.getBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, false)) {
      // Make sure to avoid clobbering the old Configurator
      @SuppressWarnings("rawtypes")
      final ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> prevConfigurator =
          channelProvider.getChannelConfigurator();
      //noinspection rawtypes
      channelProvider.setChannelConfigurator(
          new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
            @Override
            public ManagedChannelBuilder apply(ManagedChannelBuilder channelBuilder) {
              if (prevConfigurator != null) {
                channelBuilder = prevConfigurator.apply(channelBuilder);
              }
              return channelBuilder.usePlaintext();
            }
          });
    }

    if (endpointKey.equals(BIGTABLE_HOST_KEY)) {
      String channelCount = configuration.get(BIGTABLE_DATA_CHANNEL_COUNT_KEY);
      if (!Strings.isNullOrEmpty(channelCount)) {
        channelProvider.setPoolSize(Integer.parseInt(channelCount));
      }
    }

    // TODO: remove this once https://github.com/googleapis/gax-java/pull/1355 is resolved
    // Workaround performance issues due to the default executor in gax
    {
      final ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> prevConfigurator =
          channelProvider.getChannelConfigurator();

      channelProvider.setChannelConfigurator(
          new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
            @Override
            public ManagedChannelBuilder apply(ManagedChannelBuilder channelBuilder) {
              if (prevConfigurator != null) {
                channelBuilder = prevConfigurator.apply(channelBuilder);
              }
              return channelBuilder.executor(null);
            }
          });
    }

    stubSettings.setTransportChannelProvider(channelProvider.build());
  }

  private void configureHeaderProvider(StubSettings.Builder<?, ?> stubSettings) {
    List<String> userAgentParts = Lists.newArrayList();
    userAgentParts.add("hbase-" + VersionInfo.getVersion());
    userAgentParts.add("bigtable-" + BigtableHBaseVersion.getVersion());
    userAgentParts.add("jdk-" + System.getProperty("java.specification.version"));

    String customUserAgent = configuration.get(CUSTOM_USER_AGENT_KEY);
    if (customUserAgent != null) {
      userAgentParts.add(customUserAgent);
    }

    String userAgent = Joiner.on(",").join(userAgentParts);

    stubSettings.setHeaderProvider(FixedHeaderProvider.create(USER_AGENT_KEY.name(), userAgent));
  }

  private void configureCredentialProvider(StubSettings.Builder<?, ?> stubSettings)
      throws IOException {

    // This preserves user defined Credentials
    if (configuration instanceof BigtableExtendedConfiguration) {
      Credentials credentials = ((BigtableExtendedConfiguration) configuration).getCredentials();
      stubSettings.setCredentialsProvider(FixedCredentialsProvider.create(credentials));

    } else if (Boolean.parseBoolean(configuration.get(BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY))) {
      stubSettings.setCredentialsProvider(NoCredentialsProvider.create());

    } else if (!Strings.isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY))) {
      String jsonValue = configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY);
      stubSettings.setCredentialsProvider(
          FixedCredentialsProvider.create(
              GoogleCredentials.fromStream(
                  new ByteArrayInputStream(jsonValue.getBytes(StandardCharsets.UTF_8)))));

    } else if (!Strings.isNullOrEmpty(
        configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY))) {
      String keyFileLocation =
          configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY);
      stubSettings.setCredentialsProvider(
          FixedCredentialsProvider.create(
              GoogleCredentials.fromStream(new FileInputStream(keyFileLocation))));

    } else if (!Strings.isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY))) {
      String serviceAccount = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
      String keyFileLocation = configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
      Preconditions.checkState(
          !Strings.isNullOrEmpty(keyFileLocation),
          "Key file location must be specified when setting service account email");
      stubSettings.setCredentialsProvider(
          FixedCredentialsProvider.create(
              buildCredentialFromPrivateKey(serviceAccount, keyFileLocation)));
    }
  }

  private Credentials buildCredentialFromPrivateKey(
      String serviceAccountEmail, String privateKeyFile) throws IOException {

    try {
      KeyStore keyStore = KeyStore.getInstance("PKCS12");

      try (FileInputStream fin = new FileInputStream(privateKeyFile)) {
        keyStore.load(fin, "notasecret".toCharArray());
      }
      PrivateKey privateKey =
          (PrivateKey) keyStore.getKey("privatekey", "notasecret".toCharArray());

      return ServiceAccountJwtAccessCredentials.newBuilder()
          .setClientEmail(serviceAccountEmail)
          .setPrivateKey(privateKey)
          .build();
    } catch (GeneralSecurityException exception) {
      throw new RuntimeException("exception while retrieving credentials", exception);
    }
  }

  private void configureBulkMutationSettings(
      BigtableBatchingCallSettings.Builder builder,
      Optional<Duration> attemptTimeout,
      Optional<Duration> overallTimeout) {
    BatchingSettings.Builder batchingSettingsBuilder = builder.getBatchingSettings().toBuilder();

    // Start configure retries & timeouts
    configureRetryableCallSettings(builder, attemptTimeout, overallTimeout);
    // End configure retries & timeouts

    // Start configure flush triggers
    String autoFlushStr = configuration.get(BIGTABLE_BULK_AUTOFLUSH_MS_KEY);
    if (!Strings.isNullOrEmpty(autoFlushStr)) {
      long autoFlushMs = Long.parseLong(autoFlushStr);
      if (autoFlushMs == 0) {
        batchingSettingsBuilder.setDelayThreshold(null);
      } else {
        batchingSettingsBuilder.setDelayThreshold(Duration.ofMillis(autoFlushMs));
      }
    }

    String bulkMaxRowKeyCountStr = configuration.get(BIGTABLE_BULK_MAX_ROW_KEY_COUNT);
    if (!Strings.isNullOrEmpty(bulkMaxRowKeyCountStr)) {
      batchingSettingsBuilder.setElementCountThreshold(Long.parseLong(bulkMaxRowKeyCountStr));
    }

    String requestByteThresholdStr = configuration.get(BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES);
    if (!Strings.isNullOrEmpty(requestByteThresholdStr)) {
      batchingSettingsBuilder.setRequestByteThreshold(Long.valueOf(requestByteThresholdStr));
    }
    // End configure flush triggers

    // Start configure flow control
    FlowControlSettings.Builder flowControl =
        builder.getBatchingSettings().getFlowControlSettings().toBuilder();

    // Approximate max inflight rpcs in terms of outstanding elements
    String maxInflightRpcStr = configuration.get(MAX_INFLIGHT_RPCS_KEY);
    if (!Strings.isNullOrEmpty(maxInflightRpcStr)) {
      int maxInflightRpcCount = Integer.parseInt(maxInflightRpcStr);

      long bulkMaxRowKeyCount =
          Objects.requireNonNull(builder.getBatchingSettings().getElementCountThreshold());

      // TODO: either deprecate maxInflightRpcCount and expose the max outstanding elements
      // in user configuration or introduce maxInflightRpcCount to gax
      long maxInflightElements = maxInflightRpcCount * bulkMaxRowKeyCount;

      flowControl.setMaxOutstandingElementCount(maxInflightElements);
    }

    String maxMemory = configuration.get(BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY);
    if (!Strings.isNullOrEmpty(maxMemory)) {
      flowControl.setMaxOutstandingRequestBytes(Long.valueOf(maxMemory));
    }

    batchingSettingsBuilder.setFlowControlSettings(flowControl.build());
    // End configure flow control

    builder.setBatchingSettings(batchingSettingsBuilder.build());

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING))) {
      int latencyMs =
          configuration.getInt(
              BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS,
              BIGTABLE_BULK_THROTTLE_TARGET_MS_DEFAULT);

      builder.enableLatencyBasedThrottling(latencyMs);
    }
  }

  private void configureBulkReadRowsSettings(
      BigtableBulkReadRowsCallSettings.Builder builder,
      Optional<Duration> attemptTimeout,
      Optional<Duration> overallTimeout) {
    BatchingSettings.Builder bulkReadBatchingBuilder = builder.getBatchingSettings().toBuilder();

    // Start configure retries & timeouts
    configureRetryableCallSettings(builder, attemptTimeout, overallTimeout);
    // End configure retries & timeouts

    // Start config batch settings
    String bulkMaxRowKeyCountStr = configuration.get(BIGTABLE_BULK_MAX_ROW_KEY_COUNT);
    if (!Strings.isNullOrEmpty(bulkMaxRowKeyCountStr)) {
      bulkReadBatchingBuilder.setElementCountThreshold(Long.parseLong(bulkMaxRowKeyCountStr));
    }
    builder.setBatchingSettings(bulkReadBatchingBuilder.build());
    // End config batch settings

    // NOTE: autoflush, flow control settings are not currently exposed
  }

  private void configureReadRowsSettings(
      ServerStreamingCallSettings.Builder<Query, Row> readRowsSettings,
      Optional<Duration> readRowsAttemptTimeout,
      Optional<Duration> overallTimeout) {
    // Configure retries
    // NOTE: that similar but not the same as unary retry settings: per attempt timeouts don't
    // exist, instead we use READ_PARTIAL_ROW_TIMEOUT_MS as the intra-row timeout
    if (!configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, true)) {
      // user explicitly disabled retries, treat it as a non-idempotent method
      readRowsSettings.setRetryableCodes(Collections.<StatusCode.Code>emptySet());
    } else {
      // apply user user retry settings
      readRowsSettings.setRetryableCodes(
          extractRetryCodesFromConfig(readRowsSettings.getRetryableCodes()));

      // Configure backoff
      String initialElapsedBackoffMsStr = configuration.get(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY);
      if (!Strings.isNullOrEmpty(initialElapsedBackoffMsStr)) {
        long initialElapsedBackoffMs = Long.parseLong(initialElapsedBackoffMsStr);
        readRowsSettings
            .retrySettings()
            .setInitialRetryDelay(Duration.ofMillis(initialElapsedBackoffMs));

        if (initialElapsedBackoffMs
            > readRowsSettings.retrySettings().getMaxRetryDelay().toMillis()) {
          readRowsSettings
              .retrySettings()
              .setMaxRetryDelay(Duration.ofMillis(initialElapsedBackoffMs));
        }
      }

      String maxAttemptsStr = configuration.get(MAX_SCAN_TIMEOUT_RETRIES);
      if (!Strings.isNullOrEmpty(maxAttemptsStr)) {
        readRowsSettings.retrySettings().setMaxAttempts(Integer.parseInt(maxAttemptsStr));
      }
    }

    // overall timeout
    if (overallTimeout.isPresent()) {
      readRowsSettings.retrySettings().setTotalTimeout(overallTimeout.get());
    }

    Optional<Duration> perRowTimeout = Optional.absent();

    // No request deadlines for scans, instead we use intra-row timeouts
    String perRowTimeoutStr = configuration.get(READ_PARTIAL_ROW_TIMEOUT_MS);
    if (!Strings.isNullOrEmpty(perRowTimeoutStr)) {
      perRowTimeout = Optional.of(Duration.ofMillis(Long.parseLong(perRowTimeoutStr)));
    }

    // NOTE: java-bigtable doesn't currently support attempt timeouts for streaming operations
    // so we use it as a fallback to limit per row timeout instead.
    perRowTimeout = perRowTimeout.or(readRowsAttemptTimeout);

    if (perRowTimeout.isPresent()) {
      readRowsSettings
          .retrySettings()
          .setInitialRpcTimeout(perRowTimeout.get())
          .setMaxRpcTimeout(perRowTimeout.get());
    }
  }

  private void configureRetryableCallSettings(
      UnaryCallSettings.Builder<?, ?> unaryCallSettings,
      Optional<Duration> attemptTimeout,
      Optional<Duration> overallTimeout) {
    if (!configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, true)) {
      // user explicitly disabled retries, treat it as a non-idempotent method
      configureNonRetryableCallSettings(unaryCallSettings, overallTimeout);
      return;
    }

    // apply user user retry settings
    unaryCallSettings.setRetryableCodes(
        extractRetryCodesFromConfig(unaryCallSettings.getRetryableCodes()));

    // Configure backoff
    String initialElapsedBackoffMsStr = configuration.get(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY);
    if (!Strings.isNullOrEmpty(initialElapsedBackoffMsStr)) {
      long initialElapsedBackoffMs = Long.parseLong(initialElapsedBackoffMsStr);
      unaryCallSettings
          .retrySettings()
          .setInitialRetryDelay(Duration.ofMillis(initialElapsedBackoffMs));

      if (initialElapsedBackoffMs
          > unaryCallSettings.retrySettings().getMaxRetryDelay().toMillis()) {
        unaryCallSettings
            .retrySettings()
            .setMaxRetryDelay(Duration.ofMillis(initialElapsedBackoffMs));
      }
    }

    // Configure overall timeout
    if (overallTimeout.isPresent()) {
      unaryCallSettings.retrySettings().setTotalTimeout(overallTimeout.get());
    }

    // Configure attempt timeout - if the user hasn't explicitly configured it, then fallback to
    // overall timeout to match previous behavior
    Optional<Duration> effectiveAttemptTimeout = attemptTimeout.or(overallTimeout);
    if (effectiveAttemptTimeout.isPresent()) {
      unaryCallSettings.retrySettings().setInitialRpcTimeout(effectiveAttemptTimeout.get());
      unaryCallSettings.retrySettings().setMaxRpcTimeout(effectiveAttemptTimeout.get());
    }
  }

  private void configureNonRetryableCallSettings(
      UnaryCallSettings.Builder<?, ?> unaryCallSettings, Optional<Duration> timeout) {
    unaryCallSettings.setRetryableCodes(Collections.<StatusCode.Code>emptySet());
    if (timeout.isPresent()) {
      unaryCallSettings.retrySettings().setInitialRpcTimeout(timeout.get());
      unaryCallSettings.retrySettings().setMaxRpcTimeout(timeout.get());
      unaryCallSettings.retrySettings().setTotalTimeout(timeout.get());
    }
  }

  private Optional<Duration> extractDuration(String... keys) {
    for (String key : keys) {
      String timeoutStr = configuration.get(key);
      if (!Strings.isNullOrEmpty(timeoutStr)) {
        return Optional.of(Duration.ofMillis(Long.parseLong(timeoutStr)));
      }
    }
    return Optional.absent();
  }

  private Set<StatusCode.Code> extractRetryCodesFromConfig(Set<StatusCode.Code> defaultCodes) {
    Set<StatusCode.Code> codes = new HashSet<>(defaultCodes);

    String retryCodes = configuration.get(ADDITIONAL_RETRY_CODES, "");

    for (String stringCode : retryCodes.split(",")) {
      String trimmed = stringCode.trim();
      if (trimmed.isEmpty()) {
        continue;
      }

      StatusCode.Code code = StatusCode.Code.valueOf(trimmed);
      Preconditions.checkNotNull(code, String.format("Unknown status code %s found", stringCode));
      codes.add(code);
    }

    String enableDeadlineRetry = configuration.get(ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY);
    if (!Strings.isNullOrEmpty(enableDeadlineRetry)) {
      if (Boolean.parseBoolean(enableDeadlineRetry)) {
        codes.add(StatusCode.Code.DEADLINE_EXCEEDED);
      } else {
        codes.remove(StatusCode.Code.DEADLINE_EXCEEDED);
      }
    }

    return codes;
  }
}
