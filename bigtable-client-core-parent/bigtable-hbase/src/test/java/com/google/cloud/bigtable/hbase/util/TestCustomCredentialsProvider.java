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
import com.google.auth.RequestMetadataCallback;
import com.google.cloud.bigtable.hbase.BigtableOAuthCredentials;
import com.google.cloud.bigtable.hbase.wrappers.veneer.BigtableCredentialsWrapper;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestCustomCredentialsProvider {

  static class TestCredentials extends BigtableOAuthCredentials {

    public TestCredentials(Configuration conf) {
      super(conf);
    }

    public static void setMockMetadata(URI uri, Map<String, List<String>> requestMetadata) {
      mockRequestMetadata.put(uri, requestMetadata);
    }

    @Override
    public CompletableFuture<Map<String, List<String>>> getRequestMetadata(
        URI uri, Executor executor) throws IOException {
      return CompletableFuture.completedFuture(mockRequestMetadata.get(uri));
    }

    private static final Map<URI, Map<String, List<String>>> mockRequestMetadata = new HashMap<>();
  }

  static class MockRequestMetadataCallback implements RequestMetadataCallback {

    public MockRequestMetadataCallback(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void onSuccess(Map<String, List<String>> requestMetadata) {
      this.requestMetadata = requestMetadata;
      latch.countDown();
    }

    @Override
    public void onFailure(Throwable throwable) {
      error = throwable;
    }

    private final CountDownLatch latch;
    public Map<String, List<String>> requestMetadata;
    public Throwable error;
  }

  @Before
  public void setup() {
    configuration = new Configuration();
    configuration.set(BIGTABLE_CUSTOM_CREDENTIALS_CLASS_KEY, TestCredentials.class.getName());
    credentials =
        CustomCredentialsProvider.getCustomCredentials(
            TestCredentials.class.getName(), configuration);
  }

  @Test
  public void testCreation() {
    Assert.assertTrue(credentials instanceof BigtableCredentialsWrapper);
    BigtableCredentialsWrapper wrapper = (BigtableCredentialsWrapper) credentials;
    Assert.assertTrue(wrapper.getBigtableCredentials() instanceof TestCredentials);
    Assert.assertEquals(configuration, wrapper.getBigtableCredentials().getConfiguration());
  }

  @Test
  public void testGetMetadataAsync() throws InterruptedException {
    ExecutorService executors = Executors.newSingleThreadExecutor();

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, List<String>> requestMetadata = new HashMap<>();
    requestMetadata.put("request", Arrays.asList("metadata"));

    TestCredentials.setMockMetadata(URI.create("test-uri"), requestMetadata);

    MockRequestMetadataCallback mockRequestMetadataCallback =
        new MockRequestMetadataCallback(latch);

    credentials.getRequestMetadata(URI.create("test-uri"), executors, mockRequestMetadataCallback);
    // Wait for the metadata to be set.
    latch.await();

    Assert.assertEquals(requestMetadata, mockRequestMetadataCallback.requestMetadata);
    Assert.assertNull(mockRequestMetadataCallback.error);
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
