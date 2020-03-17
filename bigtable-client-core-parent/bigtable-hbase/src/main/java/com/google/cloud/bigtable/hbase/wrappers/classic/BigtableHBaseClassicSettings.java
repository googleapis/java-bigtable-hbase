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
package com.google.cloud.bigtable.hbase.wrappers.classic;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ADDITIONAL_RETRY_CODES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ALLOW_NO_TIMESTAMP_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.APP_PROFILE_ID_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_ASYNC_MUTATOR_COUNT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_AUTOFLUSH_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY;
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
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_BULK_API;
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
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_BUFFER_SIZE;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_PARTIAL_ROW_TIMEOUT_MS;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.hbase.BigtableExtendedConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.common.base.Preconditions;
import io.grpc.Status;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.VersionInfo;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableHBaseClassicSettings extends BigtableHBaseSettings {

  private final Configuration configuration;
  private final BigtableOptions bigtableOptions;

  public BigtableHBaseClassicSettings(Configuration configuration) throws IOException {
    super(configuration);
    // we can't create a defensive copy because it might be an instance
    // BigtableExtendedConfiguration
    this.configuration = configuration;
    BigtableOptions.Builder bigtableOptionsBuilder = BigtableOptions.builder();

    bigtableOptionsBuilder.setProjectId(getProjectId()).setInstanceId(getInstanceId());

    String appProfileId = configuration.get(APP_PROFILE_ID_KEY);
    if (!isNullOrEmpty(appProfileId)) {
      bigtableOptionsBuilder.setAppProfileId(appProfileId);
    }

    configureBulkOptions(bigtableOptionsBuilder);
    configureChannelOptions(bigtableOptionsBuilder);
    configureClientCallOptions(bigtableOptionsBuilder);
    configureCredentialOptions(bigtableOptionsBuilder);
    configureRetryOptions(bigtableOptionsBuilder);

    String emulatorHost = configuration.get(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY);
    if (!isNullOrEmpty(emulatorHost)) {
      bigtableOptionsBuilder.enableEmulator(emulatorHost);
    }

    String useBatchStr = configuration.get(BIGTABLE_USE_BATCH);
    if (!isNullOrEmpty(useBatchStr)) {
      bigtableOptionsBuilder.setUseBatch(Boolean.parseBoolean(useBatchStr));
    }

    this.bigtableOptions = bigtableOptionsBuilder.build();
  }

  @Override
  public String getDataHost() {
    return bigtableOptions.getDataHost();
  }

  @Override
  public String getAdminHost() {
    return bigtableOptions.getAdminHost();
  }

  @Override
  public int getPort() {
    return bigtableOptions.getPort();
  }

  @Override
  public int getBulkMaxRowCount() {
    return bigtableOptions.getBulkOptions().getBulkMaxRowKeyCount();
  }

  @Override
  public long getBatchingMaxRequestSize() {
    return bigtableOptions.getBulkOptions().getMaxMemory();
  }

  @Override
  public boolean isRetriesWithoutTimestampAllowed() {
    return bigtableOptions.getRetryOptions().allowRetriesWithoutTimestamp();
  }

  public BigtableOptions getBigtableOptions() {
    return bigtableOptions;
  }

  // ************** Private Helpers **************
  private void configureBulkOptions(BigtableOptions.Builder builder) {
    BulkOptions.Builder bulkOptionsBuilder = builder.getBulkOptions().toBuilder();

    String asyncMutatorCount = configuration.get(BIGTABLE_ASYNC_MUTATOR_COUNT_KEY);
    if (!isNullOrEmpty(asyncMutatorCount)) {
      bulkOptionsBuilder.setAsyncMutatorWorkerCount(Integer.parseInt(asyncMutatorCount));
    }

    String useBulkApiStr = configuration.get(BIGTABLE_USE_BULK_API);
    if (!isNullOrEmpty(useBulkApiStr)) {
      bulkOptionsBuilder.setUseBulkApi(Boolean.parseBoolean(useBulkApiStr));
    }

    String bulkMaxRowKeyCountStr = configuration.get(BIGTABLE_BULK_MAX_ROW_KEY_COUNT);
    if (!isNullOrEmpty(bulkMaxRowKeyCountStr)) {
      bulkOptionsBuilder.setBulkMaxRowKeyCount(Integer.parseInt(bulkMaxRowKeyCountStr));
    }

    String bulkMaxRequestSize = configuration.get(BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES);
    if (!isNullOrEmpty(bulkMaxRequestSize)) {
      bulkOptionsBuilder.setBulkMaxRequestSize(Long.parseLong(bulkMaxRequestSize));
    }

    String autoFlushMs = configuration.get(BIGTABLE_BULK_AUTOFLUSH_MS_KEY);
    if (!isNullOrEmpty(autoFlushMs)) {
      bulkOptionsBuilder.setAutoflushMs(Long.parseLong(autoFlushMs));
    }

    String maxInFlightRpcStr = configuration.get(MAX_INFLIGHT_RPCS_KEY);
    if (!isNullOrEmpty(maxInFlightRpcStr)) {
      bulkOptionsBuilder.setMaxInflightRpcs(Integer.parseInt(maxInFlightRpcStr));
    }

    String maxMemoryStr = configuration.get(BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY);
    if (!isNullOrEmpty(maxMemoryStr)) {
      bulkOptionsBuilder.setMaxMemory(Long.parseLong(maxMemoryStr));
    }

    if (Boolean.parseBoolean(configuration.get(BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING))) {
      String bufferedMutatorThresholdMsStr =
          configuration.get(BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS);

      if (!isNullOrEmpty(bufferedMutatorThresholdMsStr)) {
        int mutatorThresholdMs = Integer.parseInt(bufferedMutatorThresholdMsStr);
        LOG.info(
            "Bigtable mutation latency throttling enabled with threshold %d", mutatorThresholdMs);
        bulkOptionsBuilder.enableBulkMutationThrottling();
        bulkOptionsBuilder.setBulkMutationRpcTargetMs(mutatorThresholdMs);
      } else {
        LOG.info(
            "Bigtable mutation latency throttling is not enabled due to %s is yet not set",
            BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS);
      }
    }

    builder.setBulkOptions(bulkOptionsBuilder.build());
  }

  private void configureChannelOptions(BigtableOptions.Builder builder) {

    String dataHostOverride = configuration.get(BIGTABLE_HOST_KEY);
    if (!isNullOrEmpty(dataHostOverride)) {
      LOG.debug("API Data endpoint host %s.", dataHostOverride);
      builder.setDataHost(dataHostOverride);
    }

    String adminHostOverride = configuration.get(BIGTABLE_ADMIN_HOST_KEY);
    if (!isNullOrEmpty(adminHostOverride)) {
      LOG.debug("Admin endpoint host %s.", adminHostOverride);
      builder.setAdminHost(adminHostOverride);
    }

    String portOverrideStr = configuration.get(BIGTABLE_PORT_KEY);
    if (!isNullOrEmpty(portOverrideStr)) {
      builder.setPort(Integer.parseInt(portOverrideStr));
    }

    String usePlaintextStr = configuration.get(BIGTABLE_USE_PLAINTEXT_NEGOTIATION);
    if (!isNullOrEmpty(usePlaintextStr)) {
      builder.setUsePlaintextNegotiation(Boolean.parseBoolean(usePlaintextStr));
    }

    String channelCountStr = configuration.get(BIGTABLE_DATA_CHANNEL_COUNT_KEY);
    if (!isNullOrEmpty(channelCountStr)) {
      builder.setDataChannelCount(Integer.parseInt(channelCountStr));
    }

    // This is primarily used by Dataflow where connections open and close often. This is a
    // performance optimization that will reduce the cost to open connections.
    String useCachedDataPoolStr = configuration.get(BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL);
    if (!isNullOrEmpty(useCachedDataPoolStr)) {
      builder.setUseCachedDataPool(Boolean.parseBoolean(useCachedDataPoolStr));
    }

    // This information is in addition to bigtable-client-core version, and jdk version.
    StringBuilder agentBuilder = new StringBuilder();
    agentBuilder.append("hbase-").append(VersionInfo.getVersion());
    String customUserAgent = configuration.get(CUSTOM_USER_AGENT_KEY);
    if (customUserAgent != null) {
      agentBuilder.append(',').append(customUserAgent);
    }
    builder.setUserAgent(agentBuilder.toString());
  }

  private void configureClientCallOptions(BigtableOptions.Builder builder) {
    CallOptionsConfig.Builder callOptionsBuilder = builder.getCallOptionsConfig().toBuilder();

    String useTimeoutStr = configuration.get(BIGTABLE_USE_TIMEOUTS_KEY);
    if (!isNullOrEmpty(useTimeoutStr)) {
      callOptionsBuilder.setUseTimeout(Boolean.parseBoolean(useTimeoutStr));
    }

    String shortRpcTimeoutMs = configuration.get(BIGTABLE_RPC_TIMEOUT_MS_KEY);
    if (!isNullOrEmpty(shortRpcTimeoutMs)) {
      callOptionsBuilder.setShortRpcTimeoutMs(Integer.parseInt(shortRpcTimeoutMs));
    }

    String longTimeoutMs = configuration.get(BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY);
    if (!isNullOrEmpty(longTimeoutMs)) {
      callOptionsBuilder.setLongRpcTimeoutMs(Integer.parseInt(longTimeoutMs));
    }

    String mutateRpcTimeoutMs = configuration.get(BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY);
    if (!isNullOrEmpty(mutateRpcTimeoutMs)) {
      callOptionsBuilder.setMutateRpcTimeoutMs(Integer.parseInt(mutateRpcTimeoutMs));
    }

    String readRowsRpcTimeoutMs = configuration.get(BIGTABLE_READ_RPC_TIMEOUT_MS_KEY);
    if (!isNullOrEmpty(readRowsRpcTimeoutMs)) {
      callOptionsBuilder.setReadRowsRpcTimeoutMs(Integer.parseInt(readRowsRpcTimeoutMs));
    }

    builder.setCallOptionsConfig(callOptionsBuilder.build());
  }

  private void configureCredentialOptions(BigtableOptions.Builder builder)
      throws FileNotFoundException {

    // This preserves user defined Credentials
    if (configuration instanceof BigtableExtendedConfiguration) {
      Credentials credentials = ((BigtableExtendedConfiguration) configuration).getCredentials();
      builder.setCredentialOptions(CredentialOptions.credential(credentials));

    } else if (Boolean.parseBoolean(configuration.get(BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY))) {
      builder.setCredentialOptions(CredentialOptions.nullCredential());
      LOG.info("Enabling the use of null credentials. This should not be used in production.");

    } else if (!isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY))) {
      String jsonValue = configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY);
      LOG.debug("Using json value");
      builder.setCredentialOptions(CredentialOptions.jsonCredentials(jsonValue));

    } else if (!isNullOrEmpty(
        configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY))) {
      String keyFileLocation =
          configuration.get(BIGTABLE_SERVICE_ACCOUNT_JSON_KEYFILE_LOCATION_KEY);
      LOG.debug("Using json keyfile: %s", keyFileLocation);
      builder.setCredentialOptions(
          CredentialOptions.jsonCredentials(new FileInputStream(keyFileLocation)));

    } else if (!isNullOrEmpty(configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY))) {
      String serviceAccount = configuration.get(BIGTABLE_SERVICE_ACCOUNT_EMAIL_KEY);
      LOG.debug("Service account %s specified.", serviceAccount);
      String keyFileLocation = configuration.get(BIGTABLE_SERVICE_ACCOUNT_P12_KEYFILE_LOCATION_KEY);
      Preconditions.checkState(
          !isNullOrEmpty(keyFileLocation),
          "Key file location must be specified when setting service account email");
      LOG.debug("Using p12 keyfile: %s", keyFileLocation);
      builder.setCredentialOptions(
          CredentialOptions.p12Credential(serviceAccount, keyFileLocation));
    }
  }

  private void configureRetryOptions(BigtableOptions.Builder builder) {
    RetryOptions.Builder retryOptionsBuilder = builder.getRetryOptions().toBuilder();

    String enableRetries = configuration.get(ENABLE_GRPC_RETRIES_KEY);
    if (!isNullOrEmpty(enableRetries)) {
      LOG.debug("gRPC retries enabled: %s", enableRetries);
      retryOptionsBuilder.setEnableRetries(Boolean.parseBoolean(enableRetries));
    }

    String allowRetriesWithoutTimestamp = configuration.get(ALLOW_NO_TIMESTAMP_RETRIES_KEY);
    if (!isNullOrEmpty(allowRetriesWithoutTimestamp)) {
      LOG.debug("allow retries without timestamp: %s", allowRetriesWithoutTimestamp);
      retryOptionsBuilder.setAllowRetriesWithoutTimestamp(
          Boolean.parseBoolean(allowRetriesWithoutTimestamp));
    }

    String retryCodes = configuration.get(ADDITIONAL_RETRY_CODES, "");
    String[] codes = retryCodes.split(",");
    for (String stringCode : codes) {
      String trimmed = stringCode.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      Status.Code code = Status.Code.valueOf(trimmed);
      LOG.debug("gRPC retry on: %s", stringCode);
      retryOptionsBuilder.addStatusToRetryOn(code);
    }

    String retryOnDeadlineExceeded = configuration.get(ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY);
    if (!isNullOrEmpty(retryOnDeadlineExceeded)) {
      LOG.debug("gRPC retry on deadline exceeded enabled: %s", retryOnDeadlineExceeded);
      retryOptionsBuilder.setRetryOnDeadlineExceeded(Boolean.parseBoolean(retryOnDeadlineExceeded));
    }

    String initialElapsedBackoffMs = configuration.get(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY);
    if (!isNullOrEmpty(initialElapsedBackoffMs)) {
      LOG.debug("gRPC retry initialElapsedBackoffMillis: %d", initialElapsedBackoffMs);
      retryOptionsBuilder.setInitialBackoffMillis(Integer.parseInt(initialElapsedBackoffMs));
    }

    String maxElapsedBackoffMs = configuration.get(MAX_ELAPSED_BACKOFF_MILLIS_KEY);
    if (!isNullOrEmpty(maxElapsedBackoffMs)) {
      LOG.debug("gRPC retry maxElapsedBackoffMillis: %s", maxElapsedBackoffMs);
      retryOptionsBuilder.setMaxElapsedBackoffMillis(Integer.parseInt(maxElapsedBackoffMs));
    }

    String readPartialRowTimeoutMs = configuration.get(READ_PARTIAL_ROW_TIMEOUT_MS);
    if (!isNullOrEmpty(readPartialRowTimeoutMs)) {
      LOG.debug("gRPC read partial row timeout (millis): %d", readPartialRowTimeoutMs);
      retryOptionsBuilder.setReadPartialRowTimeoutMillis(Integer.parseInt(readPartialRowTimeoutMs));
    }

    // TODO: remove streamingBufferSize property, its not used anymore.
    String streamingBufferSize = configuration.get(READ_BUFFER_SIZE);
    if (!isNullOrEmpty(streamingBufferSize)) {
      LOG.debug("gRPC read buffer size (count): %d", streamingBufferSize);
      retryOptionsBuilder.setStreamingBufferSize(Integer.parseInt(streamingBufferSize));
    }

    String maxScanTimeoutRetries = configuration.get(MAX_SCAN_TIMEOUT_RETRIES);
    if (!isNullOrEmpty(maxScanTimeoutRetries)) {
      LOG.debug("gRPC max scan timeout retries (count): %d", maxScanTimeoutRetries);
      retryOptionsBuilder.setMaxScanTimeoutRetries(Integer.parseInt(maxScanTimeoutRetries));
    }

    builder.setRetryOptions(retryOptionsBuilder.build());
  }
}
