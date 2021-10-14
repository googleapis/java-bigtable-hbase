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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

class TestConnection implements Connection {
  public static List<Connection> mocks = new ArrayList<>();
  private Connection connectionMock;

  public TestConnection(Configuration conf, boolean managed, ExecutorService pool, User user) {
    connectionMock = mock(Connection.class);
    mocks.add(connectionMock);
  }

  @Override
  public Configuration getConfiguration() {
    return connectionMock.getConfiguration();
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    return connectionMock.getTable(tableName);
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService executorService) throws IOException {
    return connectionMock.getTable(tableName, executorService);
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    return connectionMock.getBufferedMutator(tableName);
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams)
      throws IOException {
    return connectionMock.getBufferedMutator(bufferedMutatorParams);
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return connectionMock.getRegionLocator(tableName);
  }

  @Override
  public Admin getAdmin() throws IOException {
    return connectionMock.getAdmin();
  }

  @Override
  public void close() throws IOException {
    connectionMock.close();
  }

  @Override
  public boolean isClosed() {
    return connectionMock.isClosed();
  }

  @Override
  public void abort(String s, Throwable throwable) {
    connectionMock.abort(s, throwable);
  }

  @Override
  public boolean isAborted() {
    return connectionMock.isAborted();
  }
}

@RunWith(JUnit4.class)
public class TestMirroringConnection {

  private Configuration createConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set("hbase.client.connection.impl", MirroringConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    configuration.set(MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "1");
    configuration.set(MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "2");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.prefix-path", "/tmp/test-");
    configuration.set("google.bigtable.mirroring.write-error-log.appender.max-buffer-size", "1024");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.drop-on-overflow", "false");
    return configuration;
  }

  @Test
  public void testConnectionFactoryCreatesMirroringConnection() throws IOException {
    Configuration configuration = createConfiguration();
    Connection connection = ConnectionFactory.createConnection(configuration);
    assertThat(connection).isInstanceOf(MirroringConnection.class);
    assertThat(((MirroringConnection) connection).getPrimaryConnection())
        .isInstanceOf(TestConnection.class);
    assertThat(((MirroringConnection) connection).getSecondaryConnection())
        .isInstanceOf(TestConnection.class);
  }

  @Test
  public void testCloseClosesUnderlyingConnections() throws IOException {
    TestConnection.mocks.clear();
    Configuration configuration = createConfiguration();
    Connection connection = ConnectionFactory.createConnection(configuration);

    assertThat(TestConnection.mocks.size()).isEqualTo(2);
    connection.close();
    assertThat(connection.isClosed()).isTrue();
    verify(TestConnection.mocks.get(0), times(1)).close();
    verify(TestConnection.mocks.get(1), times(1)).close();
  }

  @Test
  public void testAbortAbortsUnderlyingConnections() throws IOException {
    TestConnection.mocks.clear();
    Configuration configuration = createConfiguration();
    Connection connection = ConnectionFactory.createConnection(configuration);

    assertThat(TestConnection.mocks.size()).isEqualTo(2);
    String expectedString = "expected";
    Throwable expectedThrowable = new Exception();
    connection.abort(expectedString, expectedThrowable);
    verify(TestConnection.mocks.get(0), times(1)).abort(expectedString, expectedThrowable);
    verify(TestConnection.mocks.get(1), times(1)).abort(expectedString, expectedThrowable);
  }
}
