/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.bigtable.hbase.util;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.BIGTABLE_CUSTOM_CREDENTIALS_CLASS_KEY;

import com.google.auth.Credentials;
import com.google.cloud.bigtable.hbase.BigtableOAuth2Credentials;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableCredentialsWrapper;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestCustomCredentialsProvider {

  public static class TestCredentials extends BigtableOAuth2Credentials {

    public TestCredentials(Configuration conf) {
      super(conf);
    }

    public static void setMockMetadata(URI uri, Map<String, List<String>> requestMetadata) {
      mockRequestMetadata.put(uri, requestMetadata);
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
      return mockRequestMetadata.get(uri);
    }

    private static final Map<URI, Map<String, List<String>>> mockRequestMetadata = new HashMap<>();
  }

  @Before
  public void setup() {
    configuration = new Configuration();
    configuration.set(BIGTABLE_CUSTOM_CREDENTIALS_CLASS_KEY, TestCredentials.class.getName());
    credentials = BigtableOAuth2Credentials.newInstance(TestCredentials.class, configuration);
  }

  @Test
  public void testCreation() {
    Assert.assertTrue(credentials instanceof BigtableCredentialsWrapper);
    BigtableCredentialsWrapper wrapper = (BigtableCredentialsWrapper) credentials;
    Assert.assertTrue(wrapper.getBigtableCredentials() instanceof TestCredentials);
    Assert.assertEquals(configuration, wrapper.getBigtableCredentials().getConfiguration());
  }

  @Test
  public void testGetMetadataSync() throws IOException {

    Map<String, List<String>> requestMetadata = new HashMap<>();
    requestMetadata.put("request", Arrays.asList("metadata"));
    TestCredentials.setMockMetadata(URI.create("test-uri"), requestMetadata);

    Assert.assertEquals(requestMetadata, credentials.getRequestMetadata(URI.create("test-uri")));
  }

  private Configuration configuration;
  private Credentials credentials;
}
