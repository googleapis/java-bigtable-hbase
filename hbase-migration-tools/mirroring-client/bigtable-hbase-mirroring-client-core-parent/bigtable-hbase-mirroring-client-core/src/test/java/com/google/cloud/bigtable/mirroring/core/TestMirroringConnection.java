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
package com.google.cloud.bigtable.mirroring.core;

import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestMirroringConnection {
  private Connection connection;

  @Before
  public void setUp() throws IOException {
    TestConnection.reset();
    Configuration configuration = createConfiguration();
    connection = ConnectionFactory.createConnection(configuration);
    assertThat(TestConnection.connectionMocks.size()).isEqualTo(2);
  }

  private Configuration createConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set("hbase.client.connection.impl", MirroringConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    configuration.set(
        MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    // Prefix keys have to be set because we are using the same class as primary and secondary
    // connection class.
    configuration.set(MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "primary-connection");
    configuration.set(MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "secondary-connection");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.prefix-path", "/tmp/test-");
    configuration.set("google.bigtable.mirroring.write-error-log.appender.max-buffer-size", "1024");
    configuration.set(
        "google.bigtable.mirroring.write-error-log.appender.drop-on-overflow", "false");
    return configuration;
  }

  @Test
  public void testConnectionFactoryCreatesMirroringConnection() throws IOException {
    assertThat(connection).isInstanceOf(MirroringConnection.class);
    assertThat(((MirroringConnection) connection).getPrimaryConnection())
        .isInstanceOf(TestConnection.class);
    assertThat(((MirroringConnection) connection).getSecondaryConnection())
        .isInstanceOf(TestConnection.class);
  }

  @Test
  public void testCloseClosesUnderlyingConnections() throws IOException {
    connection.close();
    assertThat(connection.isClosed()).isTrue();
    verify(TestConnection.connectionMocks.get(0), times(1)).close();
    verify(TestConnection.connectionMocks.get(1), times(1)).close();
  }

  @Test
  public void testAbortAbortsUnderlyingConnections() throws IOException {
    String expectedString = "expected";
    Throwable expectedThrowable = new Exception();
    connection.abort(expectedString, expectedThrowable);
    verify(TestConnection.connectionMocks.get(0), times(1))
        .abort(expectedString, expectedThrowable);
    verify(TestConnection.connectionMocks.get(1), times(1))
        .abort(expectedString, expectedThrowable);
  }

  @Test
  public void testConstructorTakingMirroringConfiguration() throws IOException {
    new MirroringConnection(new MirroringConfiguration(createConfiguration()), null);
  }
}
