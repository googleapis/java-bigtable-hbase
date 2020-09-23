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
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY;
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
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.veneer.metrics.MetricsApiTracerAdapterFactory;
import io.grpc.internal.GrpcUtil;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
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
    configuration.set(BigtableOptionsFactory.BIGTABLE_USE_GCJ_CLIENT, "true");
  }

  @Test
  public void testDataSettingsBasicKeys() throws IOException {
    String appProfileId = "appProfileId";
    String userAgent = "test-user-agent";
    Credentials credentials = Mockito.mock(Credentials.class);

    configuration.set(BIGTABLE_PORT_KEY, String.valueOf(TEST_PORT));
    configuration.set(APP_PROFILE_ID_KEY, appProfileId);
    configuration.setBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
    configuration.set(CUSTOM_USER_AGENT_KEY, userAgent);
    configuration.setInt(BIGTABLE_DATA_CHANNEL_COUNT_KEY, 3);
    configuration.set(BIGTABLE_USE_CACHED_DATA_CHANNEL_POOL, "true");
    configuration.set(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "true");
    configuration.set(ALLOW_NO_TIMESTAMP_RETRIES_KEY, "true");
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
    assertEquals(
        credentials, dataSettings.getStubSettings().getCredentialsProvider().getCredentials());

    assertTrue(settingUtils.isChannelPoolCachingEnabled());
    assertTrue(settingUtils.isRetriesWithoutTimestampAllowed());
    assertTrue(
        dataSettings.getStubSettings().getTracerFactory()
            instanceof MetricsApiTracerAdapterFactory);
  }

  @Test
  public void testAdminSettingsBasicKeys() throws IOException {
    String adminHost = "testadmin.example.com";
    String userAgent = "test-user-agent";
    Credentials credentials = Mockito.mock(Credentials.class);

    configuration.set(BIGTABLE_ADMIN_HOST_KEY, adminHost);
    configuration.setInt(BIGTABLE_PORT_KEY, TEST_PORT);
    configuration.set(CUSTOM_USER_AGENT_KEY, userAgent);
    configuration.setBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
    configuration.set(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "true");
    configuration = BigtableConfiguration.withCredentials(configuration, credentials);

    BigtableHBaseVeneerSettings settings =
        (BigtableHBaseVeneerSettings) BigtableHBaseSettings.create(configuration);
    BigtableTableAdminSettings adminSettings = settings.getTableAdminSettings();
    assertEquals(TEST_PROJECT_ID, adminSettings.getProjectId());
    assertEquals(TEST_INSTANCE_ID, adminSettings.getInstanceId());

    assertEquals(adminHost + ":" + TEST_PORT, adminSettings.getStubSettings().getEndpoint());
    Map<String, String> headers = adminSettings.getStubSettings().getHeaderProvider().getHeaders();
    assertTrue(headers.get(GrpcUtil.USER_AGENT_KEY.name()).contains(userAgent));
    assertEquals(
        credentials, adminSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }

  @Test
  public void testInstanceAdminSettingsBasicKeys() throws IOException {
    String userAgent = "test-user-agent";
    Credentials credentials = Mockito.mock(Credentials.class);

    configuration.set(CUSTOM_USER_AGENT_KEY, userAgent);
    configuration.set(BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, "true");
    configuration = BigtableConfiguration.withCredentials(configuration, credentials);

    BigtableHBaseVeneerSettings settings =
        (BigtableHBaseVeneerSettings) BigtableHBaseSettings.create(configuration);
    BigtableInstanceAdminSettings instanceAdminSettings = settings.getInstanceAdminSettings();
    assertEquals(TEST_PROJECT_ID, instanceAdminSettings.getProjectId());

    Map<String, String> headers =
        instanceAdminSettings.getStubSettings().getHeaderProvider().getHeaders();
    assertTrue(headers.get(GrpcUtil.USER_AGENT_KEY.name()).contains(userAgent));
    assertEquals(
        credentials,
        instanceAdminSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }

  @Test
  public void testTimeoutBeingPassed() throws IOException {
    int initialElapsedMs = 100;
    int rpcTimeoutMs = 500;
    int maxElapsedMs = 1000;
    int maxAttempt = 10;
    int readRowStreamTimeout = 30000;
    configuration.setBoolean(BIGTABLE_USE_TIMEOUTS_KEY, true);
    configuration.setInt(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY, initialElapsedMs);
    configuration.setInt(BIGTABLE_RPC_TIMEOUT_MS_KEY, rpcTimeoutMs);
    configuration.setInt(MAX_ELAPSED_BACKOFF_MILLIS_KEY, maxElapsedMs);
    configuration.setInt(READ_PARTIAL_ROW_TIMEOUT_MS, rpcTimeoutMs);
    configuration.setLong(MAX_SCAN_TIMEOUT_RETRIES, maxAttempt);
    configuration.setInt(BIGTABLE_READ_RPC_TIMEOUT_MS_KEY, readRowStreamTimeout);
    BigtableDataSettings settings =
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration))
            .getDataSettings();

    RetrySettings readRowRetrySettings =
        settings.getStubSettings().readRowSettings().getRetrySettings();
    assertEquals(initialElapsedMs, readRowRetrySettings.getInitialRetryDelay().toMillis());
    assertEquals(rpcTimeoutMs, readRowRetrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(maxElapsedMs, readRowRetrySettings.getTotalTimeout().toMillis());

    RetrySettings checkAndMutateRetrySettings =
        settings.getStubSettings().checkAndMutateRowSettings().getRetrySettings();
    assertEquals(0, checkAndMutateRetrySettings.getInitialRetryDelay().toMillis());
    assertEquals(rpcTimeoutMs, checkAndMutateRetrySettings.getTotalTimeout().toMillis());

    RetrySettings readRowsRetrySettings =
        settings.getStubSettings().readRowsSettings().getRetrySettings();
    assertEquals(initialElapsedMs, readRowsRetrySettings.getInitialRetryDelay().toMillis());
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
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration))
            .getDataSettings();
    assertTrue(
        dataSettings.getStubSettings().getCredentialsProvider() instanceof NoCredentialsProvider);
    assertNull(dataSettings.getStubSettings().getCredentialsProvider().getCredentials());

    BigtableTableAdminSettings adminSettings =
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration))
            .getTableAdminSettings();
    assertTrue(
        adminSettings.getStubSettings().getCredentialsProvider() instanceof NoCredentialsProvider);
    assertNull(adminSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }

  @Test
  public void testVerifyRetrySettings() throws IOException {
    configuration.set(BIGTABLE_USE_TIMEOUTS_KEY, "true");
    BigtableDataSettings dataSettings =
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration))
            .getDataSettings();
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
  }

  @Test
  public void testDataSettingsWithEmulator() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();
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
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();
    String emulatorHost = "localhost:" + availablePort;
    configuration.set(BIGTABLE_EMULATOR_HOST_KEY, emulatorHost);

    BigtableTableAdminSettings adminSettings =
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration))
            .getTableAdminSettings();

    assertEquals(emulatorHost, adminSettings.getStubSettings().getEndpoint());
    CredentialsProvider credProvider = adminSettings.getStubSettings().getCredentialsProvider();
    assertTrue(credProvider instanceof NoCredentialsProvider);
  }

  @Test
  public void testRpcMethodWithoutRetry() throws IOException {
    configuration.set(ENABLE_GRPC_RETRIES_KEY, "false");
    BigtableHBaseVeneerSettings settingUtils =
        ((BigtableHBaseVeneerSettings) BigtableHBaseVeneerSettings.create(configuration));
    BigtableDataSettings settings = settingUtils.getDataSettings();

    assertTrue(settings.getStubSettings().readRowsSettings().getRetryableCodes().isEmpty());

    assertRpcWithoutTimeout(settings.getStubSettings().bulkReadRowsSettings());
    assertRpcWithoutTimeout(settings.getStubSettings().bulkMutateRowsSettings());
    assertRpcWithoutTimeout(settings.getStubSettings().readRowSettings());
    assertRpcWithoutTimeout(settings.getStubSettings().mutateRowSettings());
    assertRpcWithoutTimeout(settings.getStubSettings().sampleRowKeysSettings());
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

  private void assertRpcWithoutTimeout(UnaryCallSettings unaryCallSettings) {
    assertTrue(unaryCallSettings.getRetryableCodes().isEmpty());

    assertEquals(Duration.ofHours(12), unaryCallSettings.getRetrySettings().getInitialRpcTimeout());
    assertEquals(Duration.ofHours(12), unaryCallSettings.getRetrySettings().getMaxRpcTimeout());
    assertEquals(Duration.ofHours(12), unaryCallSettings.getRetrySettings().getTotalTimeout());
  }
}
