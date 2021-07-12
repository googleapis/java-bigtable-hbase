/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAuth extends AbstractTest {
  @Test
  public void testBatchJwt() throws IOException {
    Assume.assumeTrue("Batch JWT can only run against Bigtable", sharedTestEnv.isBigtable());

    String currentEndpoint = sharedTestEnv.getConfiguration().get("google.bigtable.endpoint.host");
    Assume.assumeTrue(
        "Batch JWT test can only run in prod",
        currentEndpoint == null || "bigtable.googleapis.com".equals(currentEndpoint));

    Credentials credentials = GoogleCredentials.getApplicationDefault();

    if (credentials instanceof ServiceAccountCredentials) {
      ServiceAccountCredentials svcCreds = (ServiceAccountCredentials) credentials;
      credentials =
          ServiceAccountJwtAccessCredentials.newBuilder()
              .setClientId(svcCreds.getClientId())
              .setClientEmail(svcCreds.getClientEmail())
              .setPrivateKeyId(svcCreds.getPrivateKeyId())
              .setPrivateKey(svcCreds.getPrivateKey())
              .build();
    }

    Assume.assumeTrue(
        "Service account credentials are required",
        credentials instanceof ServiceAccountJwtAccessCredentials);

    BigtableExtendedConfiguration config =
        new BigtableExtendedConfiguration(sharedTestEnv.getConfiguration(), credentials);
    config.set("google.bigtable.use.batch", "true");

    // Prevent the test from hanging if auth fails
    config.set("google.bigtable.rpc.use.timeouts", "true");
    config.set("google.bigtable.rpc.timeout.ms", "10000");
    config.set("google.bigtable.grpc.channel.count", "1");

    // Create a new connection using JWT auth & batch settings
    try (Connection connection = BigtableConfiguration.connect(config)) {
      // Reuse the default test table
      Table table = connection.getTable(sharedTestEnv.getDefaultTableName());
      Exception actualError = null;

      // Perform any RPC
      try {
        table.get(new Get("any-key".getBytes()));
      } catch (Exception e) {
        actualError = e;
      }

      // Verify that it succeeded.
      Assert.assertNull("No error when getting a key with JWT", actualError);
    }
  }
}
