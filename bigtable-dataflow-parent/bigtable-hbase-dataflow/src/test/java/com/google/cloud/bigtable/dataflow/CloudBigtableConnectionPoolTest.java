/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflow;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CloudBigtableConnectionPool}. */
@RunWith(JUnit4.class)
public class CloudBigtableConnectionPoolTest {

  private static Configuration config = new Configuration(false);

  private static class TestCloudbigtableConnectionPool extends CloudBigtableConnectionPool {
    @Override
    protected AbstractBigtableConnection createConnection(Configuration config) throws IOException {
      return mock(AbstractBigtableConnection.class);
    }
  }

  @Test
  public void testConnectionIsPooled() throws IOException {
    TestCloudbigtableConnectionPool pool = new TestCloudbigtableConnectionPool();
    AbstractBigtableConnection entry = pool.getConnection(config, "key");
    for (int i = 0; i < 100; i++) {
      AbstractBigtableConnection newEntry = pool.getConnection(config, "key");
      assertSame(entry, newEntry);
    }
  }

  @Test
  public void testDifferentKeys() throws IOException {
    TestCloudbigtableConnectionPool pool = new TestCloudbigtableConnectionPool();
    AbstractBigtableConnection entry1 = pool.getConnection(config, "key1");
    AbstractBigtableConnection entry2 = pool.getConnection(config, "key2");
    for (int i = 0; i < 100; i++) {
      AbstractBigtableConnection newEntry1 = pool.getConnection(config, "key1");
      assertSame(entry1, newEntry1);

      AbstractBigtableConnection newEntry2 = pool.getConnection(config, "key2");
      assertSame(entry2, newEntry2);
    }
  }
}
