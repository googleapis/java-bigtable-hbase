package com.google.cloud.anviltop.hbase;


import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAnviltopOptionsFactory {

  public static final String TEST_HOST = "test-host";
  public static final int TEST_PORT = 80;
  public static final String TEST_PROJECT_ID = "project-foo";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testProjectIdIsRequired() {
    Configuration configuration = new Configuration();
    configuration.set(AnvilTopOptionsFactory.ANVILTOP_HOST_KEY, TEST_HOST);

    expectedException.expect(IllegalArgumentException.class);
    AnvilTopOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testHostIsRequired() {
    Configuration configuration = new Configuration();
    configuration.set(AnvilTopOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);

    expectedException.expect(IllegalArgumentException.class);
    AnvilTopOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testOptionsAreConstructedWithValidInput() {
    Configuration configuration = new Configuration();
    configuration.set(AnvilTopOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(AnvilTopOptionsFactory.ANVILTOP_HOST_KEY, TEST_HOST);
    AnviltopOptions options = AnvilTopOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_HOST, options.getTransportOptions().getHost());
    Assert.assertEquals(TEST_PROJECT_ID, options.getProjectId());
  }
}
