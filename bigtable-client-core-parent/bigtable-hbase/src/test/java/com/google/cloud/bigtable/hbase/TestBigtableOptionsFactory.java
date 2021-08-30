/*
 * Copyright 2015 Google LLC
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.auth.Credentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.RetryOptions;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class TestBigtableOptionsFactory {

  public static final String TEST_HOST = "localhost";
  public static final int TEST_PORT = 80;
  public static final String TEST_PROJECT_ID = "project-foo";
  public static final String TEST_INSTANCE_ID = "test-instance";

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
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
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
  public void testTracingCookie() throws IOException {
    String fakeTracingCookie = "fake-tracing-cookie";
    configuration.set(BigtableOptionsFactory.BIGTABLE_TRACING_COOKIE, fakeTracingCookie);
    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);

    assertEquals(fakeTracingCookie, options.getTracingCookie());
  }
}
