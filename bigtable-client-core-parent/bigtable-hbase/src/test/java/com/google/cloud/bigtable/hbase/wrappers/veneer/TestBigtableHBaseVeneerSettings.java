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
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_PORT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_ATTEMPT_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_TRACING_COOKIE;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.ENABLE_GRPC_RETRIES_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_INFLIGHT_RPCS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_PARTIAL_ROW_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.threeten.bp.Duration.ofMillis;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableHBaseVersion;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.veneer.metrics.MetricsApiTracerAdapterFactory;
import com.google.common.base.Optional;
import io.grpc.internal.GrpcUtil;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class TestBigtableHBaseVeneerSettings {

  private static final String TEST_HOST = "localhost";
  private static final int TEST_PORT = 80;
  private static final String TEST_PROJECT_ID = "project-foo";
  private static final String TEST_INSTANCE_ID = "test-instance";

  private Configuration configuration;

  @Before
  public void setup() {
    configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.set(BIGTABLE_ADMIN_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
  }

  @Test
  public void testDataSettingsBasicKeys() throws IOException {
    String appProfileId = "appProfileId";
    String userAgent = "test-user-agent";
    String fakeTracingCookie = "fake-tracing-cookie";
    Credentials credentials = Mockito.mock(Credentials.class);

    configuration.set(BIGTABLE_PORT_KEY, String.valueOf(TEST_PORT));
    configuration.set(APP_PROFILE_ID_KEY, appProfileId);
    configuration.setBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
    configuration.set(CUSTOM_USER_AGENT_KEY, userAgent);
    configuration.set(BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, "true");
    configuration.set(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "true");
    configuration.set(ALLOW_NO_TIMESTAMP_RETRIES_KEY, "true");
    configuration.set(BIGTABLE_TRACING_COOKIE, fakeTracingCookie);
    configuration = BigtableConfiguration.withCredentials(configuration, credentials);

    BigtableHBaseVeneerSettings settingUtils =
        (BigtableHBaseVeneerSettings) BigtableHBaseSettings.create(configuration);

    BigtableDataSettings dataSettings = settingUtils.getDataSettings();
    assertEquals(TEST_PROJECT_ID, dataSettings.getProjectId());
    assertEquals(TEST_INSTANCE_ID, dataSettings.getInstanceId());
    assertEquals(appProfileId, dataSettings.getAppProfileId());

    assertEquals(TEST_HOST, settingUtils.getDataHost());
    assertEquals(TEST_PORT, settingUtils.getPort());
    assertEquals(TEST_HOST, settingUtils.getAdminHost());

    assertEquals(TEST_HOST + ":" + TEST_PORT, dataSettings.getStubSettings().getEndpoint());
    Map<String, String> headers = dataSettings.getStubSettings().getHeaderProvider().getHeaders();
    assertTrue(headers.get(GrpcUtil.USER_AGENT_KEY.name()).contains(userAgent));
    assertTrue(
        headers
            .get(GrpcUtil.USER_AGENT_KEY.name())
            .contains("bigtable-hbase/" + BigtableHBaseVersion.getVersion()));
    assertTrue(headers.get("cookie").equals(fakeTracingCookie));
    assertEquals(
        credentials, dataSettings.getStubSettings().getCredentialsProvider().getCredentials());

    assertTrue(settingUtils.isChannelPoolCachingEnabled());
    assertTrue(settingUtils.isRetriesWithoutTimestampAllowed());
    assertTrue(
        dataSettings.getStubSettings().getTracerFactory()
            instanceof MetricsApiTracerAdapterFactory);
  }

  @Test
  public void testDataSettingsChannelPool() throws IOException {
    configuration.setInt(BIGTABLE_DATA_CHANNEL_COUNT_KEY, 3);

    BigtableHBaseVeneerSettings settings =
        (BigtableHBaseVeneerSettings) BigtableHBaseSettings.create(configuration);
    BigtableDataSettings dataSettings = settings.getDataSettings();
    InstantiatingGrpcChannelProvider transportChannelProvider =
        (InstantiatingGrpcChannelProvider)
            dataSettings.getStubSettings().getTransportChannelProvider();

    assertEquals(3, transportChannelProvider.toBuilder().getPoolSize());
  }

  @Test
  public void testAdminSettingsBasicKeys() throws IOException {
    String adminHost = "testadmin.example.com";
    String userAgent = "test-user-agent";
    String fakeTracingCookie = "fake-tracing-cookie";
    Credentials credentials = Mockito.mock(Credentials.class);

    configuration.set(BIGTABLE_ADMIN_HOST_KEY, adminHost);
    configuration.setInt(BIGTABLE_PORT_KEY, TEST_PORT);
    configuration.set(CUSTOM_USER_AGENT_KEY, userAgent);
    configuration.setBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
    configuration.set(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "true");
    configuration.set(BIGTABLE_TRACING_COOKIE, fakeTracingCookie);
    configuration = BigtableConfiguration.withCredentials(configuration, credentials);

    BigtableHBaseVeneerSettings settings =
        (BigtableHBaseVeneerSettings) BigtableHBaseSettings.create(configuration);
    BigtableTableAdminSettings adminSettings = settings.getTableAdminSettings();
    assertEquals(TEST_PROJECT_ID, adminSettings.getProjectId());
    assertEquals(TEST_INSTANCE_ID, adminSettings.getInstanceId());
    assertEquals(TEST_INSTANCE_ID, settings.getInstanceId());

    assertEquals(adminHost + ":" + TEST_PORT, adminSettings.getStubSettings().getEndpoint());
    Map<String, String> headers = adminSettings.getStubSettings().getHeaderProvider().getHeaders();
    assertTrue(headers.get(GrpcUtil.USER_AGENT_KEY.name()).contains(userAgent));
    assertTrue(headers.get("cookie").equals(fakeTracingCookie));
    assertEquals(
        credentials, adminSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }

  @Test
  public void testAdminSettingsChannelPool() throws IOException {
    // should be ignored
    configuration.setInt(BIGTABLE_DATA_CHANNEL_COUNT_KEY, 3);

    BigtableHBaseVeneerSettings settings =
        (BigtableHBaseVeneerSettings) BigtableHBaseSettings.create(configuration);
    BigtableTableAdminSettings adminSettings = settings.getTableAdminSettings();
    InstantiatingGrpcChannelProvider transportChannelProvider =
        (InstantiatingGrpcChannelProvider)
            adminSettings.getStubSettings().getTransportChannelProvider();

    assertEquals(1, transportChannelProvider.toBuilder().getPoolSize());
  }

  @Test
  public void testTimeoutBeingPassed() throws IOException {
    int initialElapsedMs = 100;
    int rpcAttemptTimeoutMs = 100;
    int rpcTimeoutMs = 500;
    int perRowTimeoutMs = 1001;
    int maxElapsedMs = 1000;
    int maxAttempt = 10;
    int readRowStreamTimeout = 30000;
    int readRowStreamAttemptTimeout = 3000;
    configuration.setBoolean(BIGTABLE_USE_TIMEOUTS_KEY, true);
    configuration.setInt(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY, initialElapsedMs);
    configuration.setInt(MAX_ELAPSED_BACKOFF_MILLIS_KEY, maxElapsedMs);
    configuration.setInt(BIGTABLE_RPC_ATTEMPT_TIMEOUT_MS_KEY, rpcAttemptTimeoutMs);
    configuration.setInt(BIGTABLE_RPC_TIMEOUT_MS_KEY, rpcTimeoutMs);

    configuration.setInt(READ_PARTIAL_ROW_TIMEOUT_MS, perRowTimeoutMs);
    configuration.setLong(MAX_SCAN_TIMEOUT_RETRIES, maxAttempt);
    configuration.setInt(BIGTABLE_READ_RPC_TIMEOUT_MS_KEY, readRowStreamTimeout);
    configuration.setInt(BIGTABLE_READ_RPC_ATTEMPT_TIMEOUT_MS_KEY, readRowStreamAttemptTimeout);

    BigtableHBaseVeneerSettings settings = BigtableHBaseVeneerSettings.create(configuration);
    BigtableDataSettings dataSettings = settings.getDataSettings();

    assertEquals(
        Optional.of(Duration.ofMillis(rpcTimeoutMs)),
        settings.getClientTimeouts().getUnaryTimeouts().getOperationTimeout());
    assertEquals(
        Optional.of(Duration.ofMillis(rpcAttemptTimeoutMs)),
        settings.getClientTimeouts().getUnaryTimeouts().getAttemptTimeout());
    assertEquals(
        Optional.of(Duration.ofMillis(readRowStreamTimeout)),
        settings.getClientTimeouts().getScanTimeouts().getOperationTimeout());
    assertEquals(
        Optional.of(Duration.ofMillis(readRowStreamAttemptTimeout)),
        settings.getClientTimeouts().getScanTimeouts().getAttemptTimeout());
    assertEquals(
        Optional.of(Duration.ofMillis(perRowTimeoutMs)),
        settings.getClientTimeouts().getScanTimeouts().getResponseTimeout());

    RetrySettings readRowRetrySettings =
        dataSettings.getStubSettings().readRowSettings().getRetrySettings();
    assertEquals(initialElapsedMs, readRowRetrySettings.getInitialRetryDelay().toMillis());
    assertEquals(rpcTimeoutMs, readRowRetrySettings.getTotalTimeout().toMillis());
    assertEquals(rpcAttemptTimeoutMs, readRowRetrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(rpcAttemptTimeoutMs, readRowRetrySettings.getMaxRpcTimeout().toMillis());

    RetrySettings checkAndMutateRetrySettings =
        dataSettings.getStubSettings().checkAndMutateRowSettings().getRetrySettings();
    assertEquals(rpcTimeoutMs, checkAndMutateRetrySettings.getTotalTimeout().toMillis());
    // CheckAndMutate is non-retriable so its rpc timeout = overall timeout
    assertEquals(rpcTimeoutMs, checkAndMutateRetrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(rpcTimeoutMs, checkAndMutateRetrySettings.getMaxRpcTimeout().toMillis());

    RetrySettings readRowsRetrySettings =
        dataSettings.getStubSettings().readRowsSettings().getRetrySettings();
    assertEquals(initialElapsedMs, readRowsRetrySettings.getInitialRetryDelay().toMillis());
    assertEquals(perRowTimeoutMs, readRowsRetrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(perRowTimeoutMs, readRowsRetrySettings.getMaxRpcTimeout().toMillis());
    assertEquals(maxAttempt, readRowsRetrySettings.getMaxAttempts());
    assertEquals(readRowStreamTimeout, readRowsRetrySettings.getTotalTimeout().toMillis());
  }

  @Test
  public void testWhenRetriesAreDisabled() throws IOException {
    configuration.setBoolean(ENABLE_GRPC_RETRIES_KEY, false);
    BigtableDataSettings dataSettings =
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration))
            .getDataSettings();
    assertTrue(
        dataSettings.getStubSettings().bulkMutateRowsSettings().getRetryableCodes().isEmpty());
    assertTrue(dataSettings.getStubSettings().readRowSettings().getRetryableCodes().isEmpty());
    assertTrue(dataSettings.getStubSettings().readRowsSettings().getRetryableCodes().isEmpty());
    assertTrue(
        dataSettings.getStubSettings().sampleRowKeysSettings().getRetryableCodes().isEmpty());
  }

  @Test
  public void testWithNullCredentials() throws IOException {
    configuration.set(ADDITIONAL_RETRY_CODES, "UNAVAILABLE,ABORTED");
    configuration.setBoolean(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);

    BigtableDataSettings dataSettings =
        BigtableHBaseVeneerSettings.create(configuration).getDataSettings();
    assertNull(dataSettings.getStubSettings().getCredentialsProvider().getCredentials());

    BigtableTableAdminSettings adminSettings =
        BigtableHBaseVeneerSettings.create(configuration).getTableAdminSettings();
    assertTrue(
        adminSettings.getStubSettings().getCredentialsProvider() instanceof NoCredentialsProvider);
    assertNull(adminSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }

  @Ignore("Re-enable this test once veneer default align with hbase")
  @Test
  public void testVerifyRetrySettings() throws IOException {
    BigtableDataSettings dataSettings =
        BigtableHBaseVeneerSettings.create(configuration).getDataSettings();
    BigtableDataSettings defaultDataSettings =
        BigtableDataSettings.newBuilder()
            .setProjectId("project-id")
            .setInstanceId("instance-id")
            .build();

    assertEquals(
        defaultDataSettings.getStubSettings().readRowsSettings().getRetryableCodes(),
        dataSettings.getStubSettings().readRowsSettings().getRetryableCodes());

    // Unary operation's RetrySettings & RetryCodes of retryable methods.
    assertRetriableOperation(
        defaultDataSettings.getStubSettings().readRowSettings(),
        dataSettings.getStubSettings().readRowSettings());

    assertRetriableOperation(
        defaultDataSettings.getStubSettings().mutateRowSettings(),
        dataSettings.getStubSettings().mutateRowSettings());

    assertRetriableOperation(
        defaultDataSettings.getStubSettings().sampleRowKeysSettings(),
        dataSettings.getStubSettings().sampleRowKeysSettings());

    // Non-streaming operation's verifying RetrySettings & RetryCodes of non-retryable methods.
    assertNonRetriableOperation(
        defaultDataSettings.getStubSettings().readModifyWriteRowSettings(),
        dataSettings.getStubSettings().readModifyWriteRowSettings());

    assertNonRetriableOperation(
        defaultDataSettings.getStubSettings().checkAndMutateRowSettings(),
        dataSettings.getStubSettings().checkAndMutateRowSettings());
  }

  @Test
  public void testBulkMutationConfiguration() throws IOException {
    long autoFlushMs = 2000L;
    long maxRowKeyCount = 100L;
    long maxInflightRPC = 500L;
    long maxMemory = 100 * 1024L;
    long bulkMaxReqSize = 20 * 1024;

    configuration.setLong(BIGTABLE_BULK_AUTOFLUSH_MS_KEY, autoFlushMs);
    configuration.setLong(BIGTABLE_BULK_MAX_ROW_KEY_COUNT, maxRowKeyCount);
    configuration.setLong(MAX_INFLIGHT_RPCS_KEY, maxInflightRPC);
    configuration.setLong(BIGTABLE_BULK_MAX_REQUEST_SIZE_BYTES, bulkMaxReqSize);
    configuration.setLong(MAX_ELAPSED_BACKOFF_MILLIS_KEY, autoFlushMs);
    configuration.setLong(BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY, maxMemory);
    configuration.setBoolean(
        BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_ENABLE_THROTTLING, true);
    configuration.setInt(
        BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_THROTTLING_THRESHOLD_MILLIS, 1234);

    BigtableHBaseVeneerSettings settingUtils =
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration));
    BigtableDataSettings settings = settingUtils.getDataSettings();

    BatchingSettings batchingSettings =
        settings.getStubSettings().bulkMutateRowsSettings().getBatchingSettings();
    assertEquals(ofMillis(autoFlushMs), batchingSettings.getDelayThreshold());
    assertEquals(Long.valueOf(bulkMaxReqSize), batchingSettings.getRequestByteThreshold());
    assertEquals(maxRowKeyCount, settingUtils.getBulkMaxRowCount());
    assertEquals(Long.valueOf(maxRowKeyCount), batchingSettings.getElementCountThreshold());
    assertEquals(
        Long.valueOf(maxInflightRPC * maxRowKeyCount),
        batchingSettings.getFlowControlSettings().getMaxOutstandingElementCount());
    assertEquals(maxMemory, settingUtils.getBatchingMaxRequestSize());

    assertTrue(
        settings.getStubSettings().bulkMutateRowsSettings().isLatencyBasedThrottlingEnabled());
    assertEquals(
        (long) settings.getStubSettings().bulkMutateRowsSettings().getTargetRpcLatencyMs(), 1234L);
  }

  @Test
  public void testDefaultThrottlingDisabled() throws IOException {
    BigtableHBaseVeneerSettings settingUtils = BigtableHBaseVeneerSettings.create(configuration);
    BigtableDataSettings settings = settingUtils.getDataSettings();
    assertFalse(
        settings.getStubSettings().bulkMutateRowsSettings().isLatencyBasedThrottlingEnabled());
    assertEquals(settings.getStubSettings().bulkMutateRowsSettings().getTargetRpcLatencyMs(), null);
  }

  @Test
  public void testDataSettingsWithEmulator() throws IOException {
    // A real port isn't required for this test; only verifying the configs can be stored and
    // retrieved.
    final int availablePort = 987654321;
    String emulatorHost = "localhost:" + availablePort;
    configuration.set(BIGTABLE_EMULATOR_HOST_KEY, emulatorHost);

    BigtableDataSettings dataSettings =
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration))
            .getDataSettings();

    assertEquals(emulatorHost, dataSettings.getStubSettings().getEndpoint());
    CredentialsProvider credProvider = dataSettings.getStubSettings().getCredentialsProvider();
    assertTrue(credProvider instanceof NoCredentialsProvider);
  }

  @Test
  public void testAdminSettingsWithEmulator() throws IOException {
    // A real port isn't required for this test; only verifying the configs can be stored and
    // retrieved.
    final int availablePort = 987654321;
    String emulatorHost = "localhost:" + availablePort;
    configuration.set(BIGTABLE_EMULATOR_HOST_KEY, emulatorHost);

    BigtableTableAdminSettings adminSettings =
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration))
            .getTableAdminSettings();

    assertEquals(emulatorHost, adminSettings.getStubSettings().getEndpoint());
    CredentialsProvider credProvider = adminSettings.getStubSettings().getCredentialsProvider();
    assertTrue(credProvider instanceof NoCredentialsProvider);
  }

  @Ignore("Re-enable this test once veneer default align with hbase")
  @Test
  public void testRpcMethodWithoutRetry() throws IOException {
    configuration.set(ENABLE_GRPC_RETRIES_KEY, "false");
    BigtableHBaseVeneerSettings settingUtils = BigtableHBaseVeneerSettings.create(configuration);
    BigtableDataSettings settings = settingUtils.getDataSettings();

    assertTrue(settings.getStubSettings().readRowsSettings().getRetryableCodes().isEmpty());

    BigtableDataSettings defaultSettings =
        BigtableDataSettings.newBuilder()
            .setProjectId(TEST_PROJECT_ID)
            .setInstanceId(TEST_INSTANCE_ID)
            .build();

    assertEquals(
        settings.getStubSettings().readRowsSettings().getRetrySettings().toString(),
        defaultSettings.getStubSettings().readRowsSettings().getRetrySettings().toString());

    assertEquals(
        settings.getStubSettings().mutateRowSettings().getRetrySettings().toString(),
        defaultSettings.getStubSettings().mutateRowSettings().getRetrySettings().toString());
  }

  private void assertRetriableOperation(UnaryCallSettings expected, UnaryCallSettings actual) {
    assertFalse(actual.getRetryableCodes().isEmpty());
    assertEquals(expected.getRetryableCodes(), actual.getRetryableCodes());

    assertRetrySettings(expected.getRetrySettings(), actual.getRetrySettings());
  }

  private void assertNonRetriableOperation(UnaryCallSettings expected, UnaryCallSettings actual) {
    assertTrue(actual.getRetryableCodes().isEmpty());

    assertRetrySettings(expected.getRetrySettings(), actual.getRetrySettings());
  }

  private void assertRetrySettings(RetrySettings expected, RetrySettings actual) {
    assertEquals(expected.getInitialRetryDelay(), actual.getInitialRetryDelay());
    assertEquals(expected.getRetryDelayMultiplier(), actual.getRetryDelayMultiplier(), 0);
    assertEquals(expected.getMaxRetryDelay(), actual.getMaxRetryDelay());
    assertEquals(expected.getMaxAttempts(), actual.getMaxAttempts());
    assertEquals(expected.getInitialRpcTimeout(), actual.getInitialRpcTimeout());
    assertEquals(expected.getMaxRpcTimeout(), actual.getMaxRpcTimeout());
    assertEquals(expected.getTotalTimeout(), actual.getTotalTimeout());
  }
}
