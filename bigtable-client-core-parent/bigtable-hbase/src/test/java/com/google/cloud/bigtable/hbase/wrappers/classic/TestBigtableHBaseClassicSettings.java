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

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.auth.Credentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class TestBigtableHBaseClassicSettings {

  private static final String TEST_HOST = "localhost";
  private static final String TEST_PROJECT_ID = "project-foo";
  private static final String TEST_INSTANCE_ID = "test-instance";

  private Configuration configuration;

  @Before
  public void setup() {
    configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, TEST_INSTANCE_ID);
  }

  @Test
  public void testProjectIdIsRequired() {
    Configuration configuration = new Configuration(false);
    configuration.unset(BigtableOptionsFactory.PROJECT_ID_KEY);

    try {
      BigtableHBaseSettings.create(configuration);
    } catch (IllegalArgumentException | IOException ex) {
      assertEquals("Project ID must be supplied via google.bigtable.project.id", ex.getMessage());
    }
  }

  @Test
  public void testHostIsRequired() {
    Configuration configuration = new Configuration(false);
    configuration.unset(BigtableOptionsFactory.BIGTABLE_HOST_KEY);

    try {
      BigtableHBaseSettings.create(configuration);
    } catch (IllegalArgumentException | IOException ex) {
      assertEquals("Project ID must be supplied via google.bigtable.project.id", ex.getMessage());
    }
  }

  @Test
  public void testInstanceIsRequired() {
    Configuration configuration = new Configuration(false);
    configuration.unset(BigtableOptionsFactory.INSTANCE_ID_KEY);

    try {
      BigtableHBaseSettings.create(configuration);
    } catch (IllegalArgumentException | IOException ex) {
      assertEquals("Project ID must be supplied via google.bigtable.project.id", ex.getMessage());
    }
  }

  @Test
  public void testConnectionKeysAreUsed() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, "data-host");
    configuration.set(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY, "admin-host");
    configuration.setInt(BigtableOptionsFactory.BIGTABLE_PORT_KEY, 1234);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_USE_PLAINTEXT_NEGOTIATION, true);
    BigtableOptions options =
        ((BigtableHBaseClassicSettings) BigtableHBaseSettings.create(configuration))
            .getBigtableOptions();

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
    configuration.setLong(BIGTABLE_BUFFERED_MUTATOR_MAX_MEMORY_KEY, 100_000L);

    BigtableOptions options =
        ((BigtableHBaseClassicSettings) BigtableHBaseSettings.create(configuration))
            .getBigtableOptions();

    assertEquals(TEST_HOST, options.getDataHost());
    assertEquals(TEST_PROJECT_ID, options.getProjectId());
    assertEquals(TEST_INSTANCE_ID, options.getInstanceId());
    assertEquals(100_000L, options.getBulkOptions().getMaxMemory());
  }

  @Test
  public void testDefaultRetryOptions() throws IOException {
    BigtableOptions options =
        ((BigtableHBaseClassicSettings) BigtableHBaseSettings.create(configuration))
            .getBigtableOptions();
    RetryOptions retryOptions = options.getRetryOptions();

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

    BigtableOptions options =
        ((BigtableHBaseClassicSettings) BigtableHBaseSettings.create(configuration))
            .getBigtableOptions();
    RetryOptions retryOptions = options.getRetryOptions();

    assertFalse(retryOptions.enableRetries());
    assertFalse(retryOptions.retryOnDeadlineExceeded());
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

    BigtableOptions options =
        ((BigtableHBaseClassicSettings) BigtableHBaseSettings.create(configuration))
            .getBigtableOptions();
    Credentials actualCreds = CredentialFactory.getCredentials(options.getCredentialOptions());

    Assert.assertSame(credentials, actualCreds);
  }
}
