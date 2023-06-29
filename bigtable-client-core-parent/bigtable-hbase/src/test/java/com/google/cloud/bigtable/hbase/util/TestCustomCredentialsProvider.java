package com.google.cloud.bigtable.hbase.util;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_CUSTOM_CREDENTIALS_CLASS_KEY;

import com.google.auth.Credentials;
import com.google.cloud.bigtable.hbase.BigtableCredentials;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableCredentialsWrapper;
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

  static class TestCredentials extends BigtableCredentials {

    public TestCredentials(Configuration conf) {
      super(conf);
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
      return null;
    }
  }

  @Test
  public void happyCase() {
    Configuration conf = new Configuration();
    conf.set(BIGTABLE_CUSTOM_CREDENTIALS_CLASS_KEY, TestCredentials.class.getName());
    Credentials creds = CustomCredentialsProvider.getCustomCredentials(
        TestCredentials.class.getName(), conf);

    Assert.assertTrue(creds instanceof BigtableCredentialsWrapper);
    BigtableCredentialsWrapper wrapper = (BigtableCredentialsWrapper)creds;
    Assert.assertTrue(wrapper.getBigtableCredentials() instanceof TestCredentials);
    Assert.assertEquals(conf, wrapper.getBigtableCredentials().getConfiguration());
  }

}
