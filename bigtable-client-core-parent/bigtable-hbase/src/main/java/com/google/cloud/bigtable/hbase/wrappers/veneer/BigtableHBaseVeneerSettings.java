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
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY;
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
import com.google.cloud.bigtable.config.BigtableVersionInfo;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.grpc.CallOptions;
import io.grpc.CallOptions.Key;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Deadline;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
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
import java.util.concurrent.TimeUnit;
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

  private final ClientOperationTimeouts clientTimeouts;

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
    this.clientTimeouts = buildCallSettings();

    this.dataSettings = buildBigtableDataSettings(clientTimeouts);
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

  public ClientOperationTimeouts getClientTimeouts() {
    return clientTimeouts;
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
  private BigtableDataSettings buildBigtableDataSettings(ClientOperationTimeouts clientTimeouts)
      throws IOException {
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

    // Configure RPCs - this happens in multiple parts:
    // - retry settings are configured here only here
    // - timeouts are split into multiple places:
    //   - timeouts for retries are configured here
    //   - if USE_TIMEOUTS is explicitly disabled, then an interceptor is added to force all
    // deadlines to 6 minutes
    //   - DataClientVeneerApi can set a flag to affect behavior of the interceptor

    configureConnectionCallTimeouts(dataBuilder.stubSettings(), clientTimeouts);

    // Complex RPC method settings
    configureBulkMutationSettings(
        dataBuilder.stubSettings().bulkMutateRowsSettings(),
        clientTimeouts.getBulkMutateTimeouts());

    configureBulkReadRowsSettings(
        dataBuilder.stubSettings().bulkReadRowsSettings(), clientTimeouts.getScanTimeouts());
    configureReadRowsSettings(
        dataBuilder.stubSettings().readRowsSettings(), clientTimeouts.getScanTimeouts());

    // RPC methods - simple
    configureNonRetryableCallSettings(
        dataBuilder.stubSettings().checkAndMutateRowSettings(), clientTimeouts.getUnaryTimeouts());
    configureNonRetryableCallSettings(
        dataBuilder.stubSettings().readModifyWriteRowSettings(), clientTimeouts.getUnaryTimeouts());

    configureRetryableCallSettings(
        dataBuilder.stubSettings().mutateRowSettings(), clientTimeouts.getUnaryTimeouts());
    configureRetryableCallSettings(
        dataBuilder.stubSettings().readRowSettings(), clientTimeouts.getUnaryTimeouts());
    configureRetryableCallSettings(
        dataBuilder.stubSettings().sampleRowKeysSettings(), clientTimeouts.getUnaryTimeouts());

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
    ImmutableMap.Builder<String, String> headersBuilder = ImmutableMap.<String, String>builder();
    List<String> userAgentParts = Lists.newArrayList();
    userAgentParts.add("hbase-" + VersionInfo.getVersion());
    userAgentParts.add("bigtable-" + BigtableVersionInfo.CLIENT_VERSION);
    userAgentParts.add("bigtable-hbase-" + BigtableHBaseVersion.getVersion());
    userAgentParts.add("jdk-" + System.getProperty("java.specification.version"));

    String customUserAgent = configuration.get(CUSTOM_USER_AGENT_KEY);
    if (customUserAgent != null) {
      userAgentParts.add(customUserAgent);
    }

    String userAgent = Joiner.on(",").join(userAgentParts);
    headersBuilder.put(USER_AGENT_KEY.name(), userAgent);

    String tracingCookie = configuration.get(BigtableOptionsFactory.BIGTABLE_TRACING_COOKIE);
    if (tracingCookie != null) {
      headersBuilder.put("cookie", tracingCookie);
    }

    stubSettings.setHeaderProvider(FixedHeaderProvider.create(headersBuilder.build()));
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

  private void configureConnectionCallTimeouts(
      StubSettings.Builder<?, ?> stubSettings, ClientOperationTimeouts clientTimeouts) {
    // Only patch settings when timeouts are disabled
    if (clientTimeouts.getUseTimeouts()) {
      return;
    }
    InstantiatingGrpcChannelProvider.Builder channelProvider =
        ((InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider()).toBuilder();

    final ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> prevConfigurator =
        channelProvider.getChannelConfigurator();

    channelProvider.setChannelConfigurator(
        new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
          @Override
          public ManagedChannelBuilder apply(ManagedChannelBuilder managedChannelBuilder) {
            if (prevConfigurator != null) {
              managedChannelBuilder = prevConfigurator.apply(managedChannelBuilder);
            }
            return managedChannelBuilder.intercept(new NoTimeoutsInterceptor());
          }
        });
    stubSettings.setTransportChannelProvider(channelProvider.build());
  }

  private void configureBulkMutationSettings(
      BigtableBatchingCallSettings.Builder builder, OperationTimeouts operationTimeouts) {
    BatchingSettings.Builder batchingSettingsBuilder = builder.getBatchingSettings().toBuilder();

    // Start configure retries & timeouts
    configureRetryableCallSettings(builder, operationTimeouts);
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
      BigtableBulkReadRowsCallSettings.Builder builder, OperationTimeouts operationTimeouts) {
    BatchingSettings.Builder bulkReadBatchingBuilder = builder.getBatchingSettings().toBuilder();

    // Start configure retries & timeouts
    configureRetryableCallSettings(builder, operationTimeouts);
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
      OperationTimeouts operationTimeouts) {

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

    // Per response timeouts (note: gax maps rpcTimeouts to response timeouts for streaming rpcs)
    if (operationTimeouts.getResponseTimeout().isPresent()) {
      readRowsSettings
          .retrySettings()
          .setInitialRpcTimeout(operationTimeouts.getResponseTimeout().get())
          .setMaxRpcTimeout(operationTimeouts.getResponseTimeout().get());
    }

    // Attempt timeouts are set in DataClientVeneerApi

    // overall timeout
    if (operationTimeouts.getOperationTimeout().isPresent()) {
      readRowsSettings
          .retrySettings()
          .setTotalTimeout(operationTimeouts.getOperationTimeout().get());
    }
  }

  private void configureRetryableCallSettings(
      UnaryCallSettings.Builder<?, ?> unaryCallSettings, OperationTimeouts operationTimeouts) {
    if (!configuration.getBoolean(ENABLE_GRPC_RETRIES_KEY, true)) {
      // user explicitly disabled retries, treat it as a non-idempotent method
      configureNonRetryableCallSettings(unaryCallSettings, operationTimeouts);
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
    if (operationTimeouts.getOperationTimeout().isPresent()) {
      unaryCallSettings
          .retrySettings()
          .setTotalTimeout(operationTimeouts.getOperationTimeout().get());
    }

    // Configure attempt timeout - if the user hasn't explicitly configured it, then fallback to
    // overall timeout to match previous behavior
    Optional<Duration> effectiveAttemptTimeout =
        operationTimeouts.getAttemptTimeout().or(operationTimeouts.getOperationTimeout());
    if (effectiveAttemptTimeout.isPresent()) {
      unaryCallSettings.retrySettings().setInitialRpcTimeout(effectiveAttemptTimeout.get());
      unaryCallSettings.retrySettings().setMaxRpcTimeout(effectiveAttemptTimeout.get());
    }
  }

  private void configureNonRetryableCallSettings(
      UnaryCallSettings.Builder<?, ?> unaryCallSettings, OperationTimeouts operationTimeouts) {
    unaryCallSettings.setRetryableCodes(Collections.<StatusCode.Code>emptySet());

    // NOTE: attempt timeouts are not configured for non-retriable rpcs
    if (operationTimeouts.getOperationTimeout().isPresent()) {
      unaryCallSettings
          .retrySettings()
          .setLogicalTimeout(operationTimeouts.getOperationTimeout().get());
    }
  }

  private ClientOperationTimeouts buildCallSettings() {
    boolean useTimeouts = configuration.getBoolean(BIGTABLE_USE_TIMEOUTS_KEY, true);

    OperationTimeouts bulkMutateTimeouts =
        new OperationTimeouts(
            Optional.<Duration>absent(),
            extractUnaryAttemptTimeout(BIGTABLE_MUTATE_RPC_ATTEMPT_TIMEOUT_MS_KEY),
            extractOverallTimeout(
                    BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY,
                    BigtableOptionsFactory.BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY)
                .or(bulkMutateDefaultTimeout()));

    OperationTimeouts scanTimeouts =
        new OperationTimeouts(
            extractDuration(READ_PARTIAL_ROW_TIMEOUT_MS),
            extractScanAttemptTimeout(),
            extractOverallTimeout(
                BIGTABLE_READ_RPC_TIMEOUT_MS_KEY,
                BigtableOptionsFactory.BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY));

    OperationTimeouts unaryTimeouts =
        new OperationTimeouts(
            Optional.<Duration>absent(),
            extractDuration(BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY),
            extractDuration(BIGTABLE_RPC_TIMEOUT_MS_KEY, MAX_ELAPSED_BACKOFF_MILLIS_KEY));

    return new ClientOperationTimeouts(
        useTimeouts, unaryTimeouts, scanTimeouts, bulkMutateTimeouts);
  }

  private Optional<Duration> extractUnaryAttemptTimeout(String... keys) {
    if (!configuration.getBoolean(BIGTABLE_USE_TIMEOUTS_KEY, false)) {
      return Optional.of(Duration.ofMinutes(6));
    }
    return extractDuration(keys);
  }

  private Optional<Duration> extractScanAttemptTimeout() {
    if (!configuration.getBoolean(BIGTABLE_USE_TIMEOUTS_KEY, false)) {
      return Optional.absent();
    }
    return extractDuration(BIGTABLE_READ_RPC_ATTEMPT_TIMEOUT_MS_KEY);
  }

  private Optional<Duration> extractOverallTimeout(String... keys) {
    if (configuration.getBoolean(BIGTABLE_USE_TIMEOUTS_KEY, true)) {
      return extractDuration(keys);
    } else {
      return extractDuration(MAX_ELAPSED_BACKOFF_MILLIS_KEY);
    }
  }

  private Optional<Duration> bulkMutateDefaultTimeout() {
    // Set 20 minutes default timeout for batch jobs.
    if (configuration.getBoolean(BIGTABLE_USE_BATCH, false)) {
      return Optional.of(Duration.ofMinutes(20));
    }
    return Optional.absent();
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

  public static class NoTimeoutsInterceptor implements ClientInterceptor {
    public static final CallOptions.Key<Boolean> SKIP_DEFAULT_ATTEMPT_TIMEOUT =
        Key.createWithDefault("SKIP_DEFAULT_ATTEMPT_TIMEOUT", false);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

      if (!callOptions.getOption(SKIP_DEFAULT_ATTEMPT_TIMEOUT)) {
        callOptions = callOptions.withDeadline(Deadline.after(6, TimeUnit.MINUTES));
      } else {
        callOptions = callOptions.withDeadline(null);
      }

      return channel.newCall(methodDescriptor, callOptions);
    }
  }

  static class ClientOperationTimeouts {
    static final ClientOperationTimeouts EMPTY =
        new ClientOperationTimeouts(
            true, OperationTimeouts.EMPTY, OperationTimeouts.EMPTY, OperationTimeouts.EMPTY);

    private final boolean useTimeouts;
    private final OperationTimeouts unaryTimeouts;
    private final OperationTimeouts scanTimeouts;
    private final OperationTimeouts bulkMutateTimeouts;

    public ClientOperationTimeouts(
        boolean useTimeouts,
        OperationTimeouts unaryTimeouts,
        OperationTimeouts scanTimeouts,
        OperationTimeouts bulkMutateTimeouts) {
      this.useTimeouts = useTimeouts;
      this.unaryTimeouts = unaryTimeouts;
      this.scanTimeouts = scanTimeouts;
      this.bulkMutateTimeouts = bulkMutateTimeouts;
    }

    public boolean getUseTimeouts() {
      return useTimeouts;
    }

    public OperationTimeouts getUnaryTimeouts() {
      return unaryTimeouts;
    }

    public OperationTimeouts getScanTimeouts() {
      return scanTimeouts;
    }

    public OperationTimeouts getBulkMutateTimeouts() {
      return bulkMutateTimeouts;
    }
  }

  static class OperationTimeouts {
    static final OperationTimeouts EMPTY =
        new OperationTimeouts(
            Optional.<Duration>absent(), Optional.<Duration>absent(), Optional.<Duration>absent());

    // responseTimeouts are only relevant to streaming RPCs, they limit the amount of timeout a
    // stream will wait for the next response message. This is synonymous with attemptTimeouts in
    // unary RPCs since they receive a single response (so its ignored).
    private final Optional<Duration> responseTimeout;
    private final Optional<Duration> attemptTimeout;
    private final Optional<Duration> operationTimeout;

    public OperationTimeouts(
        Optional<Duration> responseTimeout,
        Optional<Duration> attemptTimeout,
        Optional<Duration> operationTimeout) {
      this.responseTimeout = responseTimeout;
      this.attemptTimeout = attemptTimeout;
      this.operationTimeout = operationTimeout;
    }

    public Optional<Duration> getResponseTimeout() {
      return responseTimeout;
    }

    public Optional<Duration> getAttemptTimeout() {
      return attemptTimeout;
    }

    public Optional<Duration> getOperationTimeout() {
      return operationTimeout;
    }
  }
}
