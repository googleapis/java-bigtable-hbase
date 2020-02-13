/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import static com.google.cloud.bigtable.config.CallOptionsConfig.LONG_TIMEOUT_MS_DEFAULT;
import static com.google.cloud.bigtable.config.CallOptionsConfig.SHORT_TIMEOUT_MS_DEFAULT;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_BACKOFF_MULTIPLIER;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_INITIAL_BACKOFF_MILLIS;
import static com.google.cloud.bigtable.config.RetryOptions.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_AUTOFLUSH_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BULK_MAX_ROW_KEY_COUNT;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.INITIAL_ELAPSED_BACKOFF_MILLIS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_INFLIGHT_RPCS_KEY;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.MAX_SCAN_TIMEOUT_RETRIES;
import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.READ_PARTIAL_ROW_TIMEOUT_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.common.collect.ImmutableSet;
import io.grpc.internal.GrpcUtil;
import java.io.IOException;
import java.net.ServerSocket;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.threeten.bp.Duration;

@RunWith(JUnit4.class)
public class TestBigtableOptionsFactory {

  public static final String TEST_HOST = "localhost";
  public static final int TEST_PORT = 80;
  public static final String TEST_PROJECT_ID = "project-foo";
  public static final String TEST_INSTANCE_ID = "test-instance";

  /** RetryCodes for idempotent RPCs. */
  private static final Set<StatusCode.Code> DEFAULT_RETRY_CODES =
      ImmutableSet.of(
          StatusCode.Code.DEADLINE_EXCEEDED,
          StatusCode.Code.UNAVAILABLE,
          StatusCode.Code.ABORTED,
          StatusCode.Code.UNAUTHENTICATED);

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private Configuration configuration;

  @Before
  public void setup() {
    configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
  }

  @Test
  public void testProjectIdIsRequired() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.unset(BigtableOptionsFactory.PROJECT_ID_KEY);

    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testHostIsRequired() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.unset(BigtableOptionsFactory.BIGTABLE_HOST_KEY);

    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testInstanceIsRequired() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.unset(BigtableOptionsFactory.INSTANCE_ID_KEY);

    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testConnectionKeysAreUsed() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, "data-host");
    configuration.set(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY, "admin-host");
    configuration.setInt(BigtableOptionsFactory.BIGTABLE_PORT_KEY, 1234);
    configuration.setBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);

    assertEquals("data-host", options.getDataHost());
    assertEquals("admin-host", options.getAdminHost());
    assertEquals(1234, options.getPort());
    assertTrue(
        "BIGTABLE_USE_PLAINTEXT_NEGOTIATION was not propagated", options.usePlaintextNegotiation());
  }

  @Test
  public void testOptionsAreConstructedWithValidInput() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);
    assertEquals(TEST_HOST, options.getDataHost());
    assertEquals(TEST_PROJECT_ID, options.getProjectId());
    assertEquals(TEST_INSTANCE_ID, options.getInstanceId());
  }

  @Test
  public void testDefaultRetryOptions() throws IOException {
    RetryOptions retryOptions =
        BigtableOptionsFactory.fromConfiguration(configuration).getRetryOptions();
    assertEquals(RetryOptions.DEFAULT_ENABLE_GRPC_RETRIES, retryOptions.enableRetries());
    assertEquals(
        RetryOptions.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS, retryOptions.getMaxElapsedBackoffMillis());
    assertEquals(
        RetryOptions.DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS,
        retryOptions.getReadPartialRowTimeoutMillis());
    assertEquals(
        RetryOptions.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES, retryOptions.getMaxScanTimeoutRetries());
  }

  @Test
  public void testSettingRetryOptions() throws IOException {
    configuration.set(BigtableOptionsFactory.ENABLE_GRPC_RETRIES_KEY, "false");
    configuration.set(BigtableOptionsFactory.ENABLE_GRPC_RETRY_DEADLINEEXCEEDED_KEY, "false");
    configuration.set(BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY, "111");
    configuration.set(BigtableOptionsFactory.READ_PARTIAL_ROW_TIMEOUT_MS, "123");
    RetryOptions retryOptions =
        BigtableOptionsFactory.fromConfiguration(configuration).getRetryOptions();
    assertEquals(false, retryOptions.enableRetries());
    assertEquals(false, retryOptions.retryOnDeadlineExceeded());
    assertEquals(111, retryOptions.getMaxElapsedBackoffMillis());
    assertEquals(123, retryOptions.getReadPartialRowTimeoutMillis());
  }

  @Test
  public void testExplicitCredentials() throws IOException, GeneralSecurityException {
    Credentials credentials = Mockito.mock(Credentials.class);
    configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration = BigtableConfiguration.withCredentials(configuration, credentials);

    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);

    Credentials actualCreds = CredentialFactory.getCredentials(options.getCredentialOptions());
    Assert.assertSame(credentials, actualCreds);
  }

  @Test
  public void testLongOperationsTimeouts() throws IOException {
    String longTimeout = "10000";
    configuration.set(BigtableOptionsFactory.BIGTABLE_LONG_RPC_TIMEOUT_MS_KEY, longTimeout);
    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);

    assertEquals(
        Integer.parseInt(longTimeout), options.getCallOptionsConfig().getMutateRpcTimeoutMs());
    assertEquals(
        Integer.parseInt(longTimeout), options.getCallOptionsConfig().getReadStreamRpcTimeoutMs());

    String readTimeout = "20000";
    configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_MUTATE_RPC_TIMEOUT_MS_KEY, longTimeout);
    configuration.set(BigtableOptionsFactory.BIGTABLE_READ_RPC_TIMEOUT_MS_KEY, readTimeout);
    options = BigtableOptionsFactory.fromConfiguration(configuration);

    assertEquals(LONG_TIMEOUT_MS_DEFAULT, options.getCallOptionsConfig().getLongRpcTimeoutMs());
    assertEquals(
        Integer.parseInt(longTimeout), options.getCallOptionsConfig().getMutateRpcTimeoutMs());
    assertEquals(
        Integer.parseInt(readTimeout), options.getCallOptionsConfig().getReadStreamRpcTimeoutMs());
  }

  @Test
  public void testDataSettingsBasicKeys() throws IOException {
    String appProfileId = "appProfileId";
    String userAgent = "test-user-agent";
    Credentials credentials = Mockito.mock(Credentials.class);

    configuration.set(BigtableOptionsFactory.BIGTABLE_PORT_KEY, String.valueOf(TEST_PORT));
    configuration.set(BigtableOptionsFactory.APP_PROFILE_ID_KEY, appProfileId);
    configuration.setBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
    configuration.set(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, userAgent);
    configuration = BigtableConfiguration.withCredentials(configuration, credentials);

    BigtableDataSettings dataSettings =
        BigtableOptionsFactory.createBigtableDataSettings(configuration);
    assertEquals(TEST_PROJECT_ID, dataSettings.getProjectId());
    assertEquals(TEST_INSTANCE_ID, dataSettings.getInstanceId());
    assertEquals(appProfileId, dataSettings.getAppProfileId());

    assertEquals(TEST_HOST + ":" + TEST_PORT, dataSettings.getStubSettings().getEndpoint());
    Map<String, String> headers = dataSettings.getStubSettings().getHeaderProvider().getHeaders();
    assertTrue(headers.get(GrpcUtil.USER_AGENT_KEY.name()).contains(userAgent));
    assertEquals(
        credentials, dataSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }

  @Test
  public void testAdminSettingsBasicKeys() throws IOException {
    String adminHost = "testadmin.example.com";
    String userAgent = "test-user-agent";
    Credentials credentials = Mockito.mock(Credentials.class);

    configuration.set(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY, adminHost);
    configuration.setInt(BigtableOptionsFactory.BIGTABLE_PORT_KEY, TEST_PORT);
    configuration.set(BigtableOptionsFactory.CUSTOM_USER_AGENT_KEY, userAgent);
    configuration.setBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
    configuration = BigtableConfiguration.withCredentials(configuration, credentials);

    BigtableTableAdminSettings adminSettings =
        BigtableOptionsFactory.createBigtableTableAdminSettings(configuration);
    assertEquals(TEST_PROJECT_ID, adminSettings.getProjectId());
    assertEquals(TEST_INSTANCE_ID, adminSettings.getInstanceId());

    assertEquals(adminHost + ":" + TEST_PORT, adminSettings.getStubSettings().getEndpoint());
    Map<String, String> headers = adminSettings.getStubSettings().getHeaderProvider().getHeaders();
    assertTrue(headers.get(GrpcUtil.USER_AGENT_KEY.name()).contains(userAgent));
    assertEquals(
        credentials, adminSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }

  @Test
  public void testRetriesWithoutTimestamp() throws IOException {
    configuration.setBoolean(BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
    configuration.setBoolean(BigtableOptionsFactory.ALLOW_NO_TIMESTAMP_RETRIES_KEY, true);

    try {
      BigtableOptionsFactory.createBigtableDataSettings(configuration);
      Assert.fail("BigtableDataSettings should not support retries without timestamp");
    } catch (UnsupportedOperationException actualException) {

      assertEquals("Retries without Timestamp does not support yet.", actualException.getMessage());
    }
  }

  @Test
  public void testWhenRetriesAreDisabled() throws IOException {
    configuration.setBoolean(BigtableOptionsFactory.ENABLE_GRPC_RETRIES_KEY, false);

    try {
      BigtableOptionsFactory.createBigtableDataSettings(configuration);
      Assert.fail("Should fail when retries are disabled");
    } catch (IllegalStateException exception) {
      assertEquals("Disabling retries is not currently supported.", exception.getMessage());
    }
  }

  @Test
  public void testWithNullCredentials() throws IOException {
    configuration.set(BigtableOptionsFactory.ADDITIONAL_RETRY_CODES, "UNAVAILABLE,ABORTED");
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);

    BigtableDataSettings dataSettings =
        BigtableOptionsFactory.createBigtableDataSettings(configuration);
    assertTrue(
        dataSettings.getStubSettings().getCredentialsProvider() instanceof NoCredentialsProvider);
    assertNull(dataSettings.getStubSettings().getCredentialsProvider().getCredentials());

    BigtableTableAdminSettings adminSettings =
        BigtableOptionsFactory.createBigtableTableAdminSettings(configuration);
    assertTrue(
        adminSettings.getStubSettings().getCredentialsProvider() instanceof NoCredentialsProvider);
    assertNull(adminSettings.getStubSettings().getCredentialsProvider().getCredentials());
  }

  @Test
  public void testVerifyRetrySettings() throws IOException {
    BigtableDataSettings dataSettings =
        BigtableOptionsFactory.createBigtableDataSettings(configuration);

    assertEquals(
        DEFAULT_RETRY_CODES, dataSettings.getStubSettings().readRowsSettings().getRetryableCodes());

    // Unary operation's RetrySettings & RetryCodes of retryable methods.
    verifyRetry(dataSettings.getStubSettings().readRowSettings().getRetrySettings());
    assertEquals(
        DEFAULT_RETRY_CODES, dataSettings.getStubSettings().readRowSettings().getRetryableCodes());

    verifyRetry(dataSettings.getStubSettings().readRowSettings().getRetrySettings());
    assertEquals(
        DEFAULT_RETRY_CODES,
        dataSettings.getStubSettings().mutateRowSettings().getRetryableCodes());

    verifyRetry(dataSettings.getStubSettings().sampleRowKeysSettings().getRetrySettings());
    assertEquals(
        DEFAULT_RETRY_CODES,
        dataSettings.getStubSettings().sampleRowKeysSettings().getRetryableCodes());

    // Non-streaming operation's verifying RetrySettings & RetryCodes of non-retryable methods.
    // readModifyWriteRowSettings
    verifyDisabledRetry(
        dataSettings.getStubSettings().readModifyWriteRowSettings().getRetrySettings());
    assertTrue(
        dataSettings.getStubSettings().readModifyWriteRowSettings().getRetryableCodes().isEmpty());

    // checkAndMutateRowSettings
    verifyDisabledRetry(
        dataSettings.getStubSettings().checkAndMutateRowSettings().getRetrySettings());
    assertTrue(
        dataSettings.getStubSettings().checkAndMutateRowSettings().getRetryableCodes().isEmpty());
  }

  private void verifyRetry(RetrySettings retrySettings) {
    assertEquals(DEFAULT_INITIAL_BACKOFF_MILLIS, retrySettings.getInitialRetryDelay().toMillis());
    assertEquals(DEFAULT_BACKOFF_MULTIPLIER, retrySettings.getRetryDelayMultiplier(), 0);
    assertEquals(DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS, retrySettings.getMaxRetryDelay().toMillis());
    assertEquals(0, retrySettings.getMaxAttempts());
    assertEquals(360_000, retrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(360_000, retrySettings.getMaxRpcTimeout().toMillis());
    assertEquals(60_000, retrySettings.getTotalTimeout().toMillis());
  }

  private void verifyDisabledRetry(RetrySettings retrySettings) {
    assertEquals(Duration.ZERO, retrySettings.getInitialRetryDelay());
    assertEquals(1, retrySettings.getRetryDelayMultiplier(), 0);
    assertEquals(Duration.ZERO, retrySettings.getMaxRetryDelay());
    assertEquals(1, retrySettings.getMaxAttempts());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, retrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, retrySettings.getMaxRpcTimeout().toMillis());
    assertEquals(SHORT_TIMEOUT_MS_DEFAULT, retrySettings.getTotalTimeout().toMillis());
    assertEquals(1, retrySettings.getMaxAttempts());
  }

  @Test
  public void testBulkMutationConfiguration() throws IOException {
    long autoFlushMs = 2000L;
    long maxRowKeyCount = 100L;
    long maxInflightRPC = 500L;
    long maxMemory = 100 * 1024L;

    configuration.setLong(BIGTABLE_BULK_AUTOFLUSH_MS_KEY, autoFlushMs);
    configuration.setLong(BIGTABLE_BULK_MAX_ROW_KEY_COUNT, maxRowKeyCount);
    configuration.setLong(MAX_INFLIGHT_RPCS_KEY, maxInflightRPC);
    configuration.setLong(BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY, maxMemory);

    BigtableDataSettings settings =
        BigtableOptionsFactory.createBigtableDataSettings(configuration);

    BatchingSettings batchingSettings =
        settings.getStubSettings().bulkMutateRowsSettings().getBatchingSettings();
    assertEquals(autoFlushMs, batchingSettings.getDelayThreshold().toMillis());
    assertEquals(maxRowKeyCount, batchingSettings.getElementCountThreshold().longValue());
    assertEquals(
        maxInflightRPC * maxRowKeyCount,
        batchingSettings.getFlowControlSettings().getMaxOutstandingElementCount().longValue());
    assertEquals(
        maxMemory,
        batchingSettings.getFlowControlSettings().getMaxOutstandingRequestBytes().longValue());
  }

  @Test
  public void testReadRowsStreamService() throws IOException {
    long initialRetryDelay = 500L;
    long rpcTimeout = 1000L;
    long maxAttempt = 10L;
    long totalTimeout = 40_000;
    configuration.setBoolean(BIGTABLE_USE_TIMEOUTS_KEY, true);
    configuration.setLong(INITIAL_ELAPSED_BACKOFF_MILLIS_KEY, initialRetryDelay);
    configuration.setLong(MAX_SCAN_TIMEOUT_RETRIES, maxAttempt);
    configuration.setLong(READ_PARTIAL_ROW_TIMEOUT_MS, rpcTimeout);
    configuration.setLong(BIGTABLE_READ_RPC_TIMEOUT_MS_KEY, totalTimeout);

    BigtableDataSettings dataSettings =
        BigtableOptionsFactory.createBigtableDataSettings(configuration);
    RetrySettings retrySettings =
        dataSettings.getStubSettings().readRowsSettings().getRetrySettings();
    assertEquals(initialRetryDelay, retrySettings.getInitialRetryDelay().toMillis());
    assertEquals(maxAttempt, retrySettings.getMaxAttempts());
    assertEquals(rpcTimeout, retrySettings.getInitialRpcTimeout().toMillis());
    assertEquals(rpcTimeout, retrySettings.getMaxRpcTimeout().toMillis());
    assertEquals(totalTimeout, retrySettings.getTotalTimeout().toMillis());
  }

  @Test
  public void testDataSettingsWithEmulator() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    final int availablePort = serverSocket.getLocalPort();
    serverSocket.close();
    String emulatorHost = "localhost:" + availablePort;
    configuration.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, emulatorHost);

    BigtableDataSettings dataSettings =
        BigtableOptionsFactory.createBigtableDataSettings(configuration);

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
    configuration.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, emulatorHost);

    BigtableTableAdminSettings adminSettings =
        BigtableOptionsFactory.createBigtableTableAdminSettings(configuration);

    assertEquals(emulatorHost, adminSettings.getStubSettings().getEndpoint());
    CredentialsProvider credProvider = adminSettings.getStubSettings().getCredentialsProvider();
    assertTrue(credProvider instanceof NoCredentialsProvider);
  }
}
