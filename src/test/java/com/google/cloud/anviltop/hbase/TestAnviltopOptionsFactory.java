package com.google.cloud.anviltop.hbase;


import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAnviltopOptionsFactory {

  public static final String TEST_API_ENDPOINT_URL = "http://somehost:1000/api/v1/foo";
  public static final String TEST_PROJECT_ID = "project-foo";

  @Test(expected = IllegalArgumentException.class)
  public void testProjectIdIsRequired() {
    Configuration configuration = new Configuration();
    configuration.set(AnvilTopOptionsFactory.API_ENDPOINT_KEY, TEST_API_ENDPOINT_URL);
    AnvilTopOptionsFactory.fromConfiguration(configuration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEndpointUrlIsRequired() {
    Configuration configuration = new Configuration();
    configuration.set(AnvilTopOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    AnvilTopOptionsFactory.fromConfiguration(configuration);
  }

  @Test
  public void testOptionsAreConstructedWithValidInput() {
    Configuration configuration = new Configuration();
    configuration.set(AnvilTopOptionsFactory.PROJECT_ID_KEY, TEST_PROJECT_ID);
    configuration.set(AnvilTopOptionsFactory.API_ENDPOINT_KEY, TEST_API_ENDPOINT_URL);
    AnviltopOptions options = AnvilTopOptionsFactory.fromConfiguration(configuration);
    Assert.assertEquals(TEST_API_ENDPOINT_URL, options.getApiEndpoint());
    Assert.assertEquals(TEST_PROJECT_ID, options.getProjectId());
  }
}
