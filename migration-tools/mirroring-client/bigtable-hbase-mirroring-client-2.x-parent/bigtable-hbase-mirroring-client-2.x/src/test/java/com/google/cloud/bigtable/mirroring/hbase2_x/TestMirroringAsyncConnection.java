/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase2_x;

import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.mirroring.core.MirroringConnection;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncAdminBuilder;
import org.apache.hadoop.hbase.client.AsyncBufferedMutatorBuilder;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.ScanResultConsumer;
import org.apache.hadoop.hbase.client.TestRegistry;
import org.apache.hadoop.hbase.security.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

class TestAsyncConnection implements AsyncConnection {
  public TestAsyncConnection(
      Configuration conf,
      /* AsyncRegion - see comments in MirroringAsyncConnection */ Object o,
      String clusterId,
      User user) {}

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearRegionLocationCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncTableBuilder<AdvancedScanResultConsumer> getTableBuilder(TableName tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncTableBuilder<ScanResultConsumer> getTableBuilder(
      TableName tableName, ExecutorService executorService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder() {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder(ExecutorService executorService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(
      TableName tableName, ExecutorService executorService) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Hbck> getHbck() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Hbck getHbck(ServerName serverName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }
}

@RunWith(JUnit4.class)
public class TestMirroringAsyncConnection {
  @Test
  public void testConnectionFactoryCreatesMirroringAsyncConnection()
      throws InterruptedException, ExecutionException, TimeoutException {
    Configuration configuration = new Configuration();
    configuration.set("hbase.client.registry.impl", TestRegistry.class.getCanonicalName());
    configuration.set(
        "hbase.client.async.connection.impl", TestAsyncConnection.class.getCanonicalName());
    configuration.set(
        "google.bigtable.mirroring.primary-client.connection.impl",
        MirroringConnection.class.getCanonicalName());
    configuration.set(
        "google.bigtable.mirroring.secondary-client.connection.impl",
        MirroringConnection.class.getCanonicalName());

    configuration.set(
        "google.bigtable.mirroring.primary-client.async.connection.impl",
        MirroringAsyncConnection.class.getCanonicalName());
    configuration.set(
        "google.bigtable.mirroring.secondary-client.async.connection.impl",
        MirroringAsyncConnection.class.getCanonicalName());

    configuration.set("google.bigtable.mirroring.primary-client.prefix", "prefix");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.prefix-path", "/tmp/test-");
    configuration.set("google.bigtable.mirroring.write-error-log.appender.max-buffer-size", "1024");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.drop-on-overflow", "false");
    AsyncConnection connection =
        ConnectionFactory.createAsyncConnection(configuration).get(1, TimeUnit.SECONDS);
    assertTrue(connection instanceof TestAsyncConnection);
  }
}
