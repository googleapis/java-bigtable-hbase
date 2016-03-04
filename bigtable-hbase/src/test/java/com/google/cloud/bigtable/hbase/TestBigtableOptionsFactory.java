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


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.RetryOptions;

@RunWith(JUnit4.class)
public class TestBigtableOptionsFactory {

  public static final String TEST_HOST = "localhost";
  public static final int TEST_PORT = 80;
  public static final String TEST_PROJECT_ID = "project-foo";
  public static final String TEST_ZONE_NAME = "test-zone";
  public static final String TEST_CLUSTER_NAME = "test-cluster";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private Configuration configuration;

  @Before
  public void setup() {
    configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.CLUSTER_KEY, TEST_CLUSTER_NAME);
    configuration.set(BigtableOptionsFactory.ZONE_KEY, TEST_ZONE_NAME);
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
    configuration.unset(BigtableOptionsFactory.BIGTABLE_DATA_HOST_KEY);

    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testClusterIsRequired() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.unset(BigtableOptionsFactory.CLUSTER_KEY);

    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testZoneIsRequired() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.unset(BigtableOptionsFactory.ZONE_KEY);

    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testAdminHostKeyIsUsed() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.BIGTABLE_TABLE_ADMIN_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_HOST, options.getDataHost());
  }

  @Test
  public void testOptionsAreConstructedWithValidInput() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_HOST, options.getDataHost());
    Assert.assertEquals(TEST_PROJECT_ID, options.getProjectId());
    Assert.assertEquals(TEST_CLUSTER_NAME, options.getClusterId());
    Assert.assertEquals(TEST_ZONE_NAME, options.getZoneId());
  }

  @Test
  public void testMinGoodRefreshTime() throws IOException{
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    configuration.setLong(BigtableOptionsFactory.BIGTABLE_CHANNEL_TIMEOUT_MS_KEY, 60000);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testBadRefreshTime() throws IOException{
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    configuration.setLong(BigtableOptionsFactory.BIGTABLE_CHANNEL_TIMEOUT_MS_KEY, 59999);
    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testZeroRefreshTime() throws IOException{
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    configuration.setLong(BigtableOptionsFactory.BIGTABLE_CHANNEL_TIMEOUT_MS_KEY, 0);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testDefaultRetryOptions() throws IOException {
    RetryOptions retryOptions =
        BigtableOptionsFactory.fromConfiguration(configuration).getRetryOptions();
    assertEquals(
      RetryOptions.ENABLE_GRPC_RETRIES_DEFAULT,
      retryOptions.enableRetries());
    assertEquals(
        RetryOptions.ENABLE_GRPC_RETRY_DEADLINE_EXCEEDED_DEFAULT,
        retryOptions.retryOnDeadlineExceeded());
    assertEquals(
        RetryOptions.DEFAULT_MAX_ELAPSED_BACKOFF_MILLIS,
        retryOptions.getMaxElaspedBackoffMillis());
    assertEquals(
        RetryOptions.DEFAULT_READ_PARTIAL_ROW_TIMEOUT_MS,
        retryOptions.getReadPartialRowTimeoutMillis());
    assertEquals(
        RetryOptions.DEFAULT_MAX_SCAN_TIMEOUT_RETRIES,
        retryOptions.getMaxScanTimeoutRetries());
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
    assertEquals(111, retryOptions.getMaxElaspedBackoffMillis());
    assertEquals(123, retryOptions.getReadPartialRowTimeoutMillis());
  }
}
