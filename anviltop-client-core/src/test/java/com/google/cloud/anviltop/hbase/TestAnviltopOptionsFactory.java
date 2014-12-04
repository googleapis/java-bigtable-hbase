package com.google.cloud.anviltop.hbase;


import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestAnviltopOptionsFactory {

  public static final String TEST_HOST = "localhost";
  public static final int TEST_PORT = 80;
  public static final String TEST_PROJECT_ID = "project-foo";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testProjectIdIsRequired() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set(AnviltopOptionsFactory.ANVILTOP_HOST_KEY, TEST_HOST);

    expectedException.expect(IllegalArgumentException.class);
    AnviltopOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testHostIsRequired() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set(AnviltopOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);

    expectedException.expect(IllegalArgumentException.class);
    AnviltopOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testAdminHostKeyIsUsed() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set(AnviltopOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(AnviltopOptionsFactory.ANVILTOP_HOST_KEY, TEST_HOST);
    configuration.set(AnviltopOptionsFactory.ANVILTOP_ADMIN_HOST_KEY, TEST_HOST);
    configuration.setBoolean(AnviltopOptionsFactory.ANVILTOP_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(AnviltopOptionsFactory.ANVILTOP_NULL_CREDENTIAL_ENABLE_KEY, true);
    AnviltopOptions options = AnviltopOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_HOST, options.getAdminTransportOptions().getHost().getHostName());
  }

  @Test
  public void testOptionsAreConstructedWithValidInput() throws IOException {
    Configuration configuration = new Configuration();
    configuration.set(AnviltopOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(AnviltopOptionsFactory.ANVILTOP_HOST_KEY, TEST_HOST);
    configuration.setBoolean(AnviltopOptionsFactory.ANVILTOP_USE_SERVICE_ACCOUNTS_KEY, false);
    configuration.setBoolean(AnviltopOptionsFactory.ANVILTOP_NULL_CREDENTIAL_ENABLE_KEY, true);
    AnviltopOptions options = AnviltopOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_HOST, options.getTransportOptions().getHost().getHostName());
    Assert.assertEquals(TEST_PROJECT_ID, options.getProjectId());
  }
}
