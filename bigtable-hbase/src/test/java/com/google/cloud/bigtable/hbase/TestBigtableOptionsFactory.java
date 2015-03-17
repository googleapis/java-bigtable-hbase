package com.google.cloud.bigtable.hbase;


import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

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
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testHostIsRequired() throws IOException {
    Configuration configuration = new Configuration();
    configuration.unset(BigtableOptionsFactory.BIGTABLE_HOST_KEY);

    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testClusterIsRequired() throws IOException {
    Configuration configuration = new Configuration();
    configuration.unset(BigtableOptionsFactory.CLUSTER_KEY);

    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testZoneIsRequired() throws IOException {
    Configuration configuration = new Configuration();
    configuration.unset(BigtableOptionsFactory.ZONE_KEY);

    expectedException.expect(IllegalArgumentException.class);
    BigtableOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testAdminHostKeyIsUsed() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.set(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_HOST, options.getAdminTransportOptions().getHost().getHostName());
  }

  @Test
  public void testOptionsAreConstructedWithValidInput() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_HOST, options.getTransportOptions().getHost().getHostName());
    Assert.assertEquals(TEST_PROJECT_ID, options.getProjectId());
    Assert.assertEquals(TEST_CLUSTER_NAME, options.getCluster());
    Assert.assertEquals(TEST_ZONE_NAME, options.getZone());
  }

  @Test
  public void executorAndEventLoopGroupArePopulatedWhenDaemonizing() throws IOException {
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, TEST_HOST);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABE_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, true);
    configuration.setBoolean(BigtableOptionsFactory.ENABLE_DAEMONIZED_THREADS_KEY, true);
    BigtableOptions options = BigtableOptionsFactory.fromConfiguration(configuration);
    Assert.assertNotNull(
        "ScheduledExecutor expected.",
        options.getChannelOptions().getScheduledExecutorService());
    Assert.assertNotNull(
        "EventLoopGroup expected",
        options.getTransportOptions().getEventLoopGroup());
  }
}
