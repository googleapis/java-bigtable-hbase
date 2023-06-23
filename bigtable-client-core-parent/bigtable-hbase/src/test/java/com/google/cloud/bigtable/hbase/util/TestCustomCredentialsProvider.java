package com.google.cloud.bigtable.hbase.util;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_CUSTOM_CREDENTIALS_CLASS_KEY;

import com.google.auth.Credentials;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestCustomCredentialsProvider {

  static class TestCredentials extends Credentials {

    public Configuration conf;

    public TestCredentials(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public String getAuthenticationType() {
      return "test-auth-type";
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
      return null;
    }

    @Override
    public boolean hasRequestMetadata() {
      return false;
    }

    @Override
    public boolean hasRequestMetadataOnly() {
      return false;
    }

    @Override
    public void refresh() throws IOException {
    }
  }

  @Test
  public void happyCase() {
    Configuration conf = new Configuration();
    conf.set(BIGTABLE_CUSTOM_CREDENTIALS_CLASS_KEY, TestCredentials.class.getName());
    Credentials creds = CustomCredentialsProvider.getCustomCredentials(
        TestCredentials.class.getName(), conf);

    Assert.assertTrue(creds instanceof TestCredentials);
    Assert.assertEquals(conf, ((TestCredentials) creds).conf);
  }

}
