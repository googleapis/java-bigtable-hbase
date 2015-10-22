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

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.dataflow.CloudBigtableConnectionPool.PoolEntry;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Tests for {@link CloudBigtableConnectionPool}.
 */
@RunWith(JUnit4.class)
public class CloudBigtableConnectionPoolTest {

  private static Configuration config = new Configuration();
  private static ExecutorService executorService = MoreExecutors.newDirectExecutorService();

  private static class TestCloudbigtableConnectionPool extends CloudBigtableConnectionPool {
    private long expireTime = System.currentTimeMillis() + 100000;

    public TestCloudbigtableConnectionPool() {
      super(executorService);
    }

    @Override
    protected PoolEntry createConnection(Configuration config, String key) throws IOException {
      return new PoolEntry(key, mock(Connection.class), expireTime);
    }
  }

  @Test
  public void testConnectionIsPooled() throws IOException{
    TestCloudbigtableConnectionPool pool = new TestCloudbigtableConnectionPool();
    PoolEntry entry = pool.getConnection(config, "key");
    pool.returnConnection(entry);
    for (int i = 0; i < 100; i++) {
      PoolEntry newEntry = pool.getConnection(config, "key");
      assertSame(entry, newEntry);
      pool.returnConnection(newEntry);
    }
  }

  @Test
  public void testExpiredConnectionIsRemoved() throws IOException{
    TestCloudbigtableConnectionPool pool = new TestCloudbigtableConnectionPool();
    pool.expireTime = System.currentTimeMillis() - 10;
    PoolEntry entry = pool.getConnection(config, "key");
    pool.returnConnection(entry);
    assertNotSame(entry, pool.getConnection(config, "key"));
    verify(entry.getConnection(), times(1)).close();
  }

  @Test
  public void testConnectionsGetCycled() throws IOException{
    TestCloudbigtableConnectionPool pool = new TestCloudbigtableConnectionPool();
    PoolEntry entry1 = pool.getConnection(config, "key");
    PoolEntry entry2 = pool.getConnection(config, "key");
    assertNotSame(entry1, entry2);
    pool.returnConnection(entry1);
    pool.returnConnection(entry2);
    for (int i = 0; i < 100; i++) {
      PoolEntry newEntry = pool.getConnection(config, "key");
      assertSame(i % 2 == 0 ? entry1 : entry2, newEntry);
      pool.returnConnection(newEntry);
    }
  }

  @Test
  public void testDifferentKeys() throws IOException{
    TestCloudbigtableConnectionPool pool = new TestCloudbigtableConnectionPool();
    PoolEntry entry1 = pool.getConnection(config, "key1");
    PoolEntry entry2 = pool.getConnection(config, "key2");
    pool.returnConnection(entry1);
    pool.returnConnection(entry2);
    for (int i = 0; i < 100; i++) {
      PoolEntry newEntry1 = pool.getConnection(config, "key1");
      assertSame(entry1, newEntry1);
      pool.returnConnection(newEntry1);

      PoolEntry newEntry2 = pool.getConnection(config, "key2");
      assertSame(entry2, newEntry2);
      pool.returnConnection(newEntry2);
    }
  }
}
