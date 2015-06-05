/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BigtableOptionsFactory;

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
    configuration = new Configuration();
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(BigtableOptionsFactory.CLUSTER_KEY, TEST_CLUSTER_NAME);
    configuration.set(BigtableOptionsFactory.ZONE_KEY, TEST_ZONE_NAME);
  }

  @Test
  public void testProjectIdIsRequired() throws IOException {
    Configuration configuration = new Configuration();
    configuration.unset(BigtableOptionsFactory.PROJECT_ID_KEY);

    expectedException.expect(IllegalArgumentException.class);
    HBaseBigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testHostIsRequired() throws IOException {
    Configuration configuration = new Configuration();
    configuration.unset(BigtableOptionsFactory.BIGTABLE_HOST_KEY);

    expectedException.expect(IllegalArgumentException.class);
    HBaseBigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testClusterIsRequired() throws IOException {
    Configuration configuration = new Configuration();
    configuration.unset(BigtableOptionsFactory.CLUSTER_KEY);

    expectedException.expect(IllegalArgumentException.class);
    HBaseBigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testZoneIsRequired() throws IOException {
    Configuration configuration = new Configuration();
    configuration.unset(BigtableOptionsFactory.ZONE_KEY);

    expectedException.expect(IllegalArgumentException.class);
    HBaseBigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testAdminHostKeyIsUsed() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.BIGTABLE_TABLE_ADMIN_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    BigtableOptions options = HBaseBigtableOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_HOST, options.getTableAdminTransportOptions().getHost().getHostName());
  }

  @Test
  public void testOptionsAreConstructedWithValidInput() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    BigtableOptions options = HBaseBigtableOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_HOST, options.getDataTransportOptions().getHost().getHostName());
    Assert.assertEquals(TEST_PROJECT_ID, options.getProjectId());
    Assert.assertEquals(TEST_CLUSTER_NAME, options.getCluster());
    Assert.assertEquals(TEST_ZONE_NAME, options.getZone());
    Assert.assertNotNull(
        "ScheduledExecutor expected.",
        options.getChannelOptions().getScheduledExecutorService());
    Assert.assertNotNull(
        "EventLoopGroup expected",
        options.getDataTransportOptions().getEventLoopGroup());
  }

  @Test
  public void testMinGoodRefreshTime() throws IOException{
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    configuration.setLong(BigtableOptionsFactory.BIGTABLE_CHANNEL_TIMEOUT_MS_KEY, 60000);
    HBaseBigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testBadRefreshTime() throws IOException{
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    configuration.setLong(BigtableOptionsFactory.BIGTABLE_CHANNEL_TIMEOUT_MS_KEY, 59999);
    expectedException.expect(IllegalArgumentException.class);
    HBaseBigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testZeroRefreshTime() throws IOException{
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    configuration.setLong(BigtableOptionsFactory.BIGTABLE_CHANNEL_TIMEOUT_MS_KEY, 0);
    HBaseBigtableOptionsFactory.fromConfiguration(configuration);
  }

}
