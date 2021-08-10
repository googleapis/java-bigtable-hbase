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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
  public TestConnection(Configuration conf, boolean managed, ExecutorService pool, User user) {}

  @Override
  public Configuration getConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService executorService) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams bufferedMutatorParams)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Admin getAdmin() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isClosed() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void abort(String s, Throwable throwable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAborted() {
    throw new UnsupportedOperationException();
  }
}

@RunWith(JUnit4.class)
public class TestMirroringConnection {

  @Test
  public void testConnectionFactoryCreatesMirroringConnection() throws IOException {
    Configuration testConfiguration = new Configuration();
    testConfiguration.set("hbase.client.connection.impl", TestConnection.class.getCanonicalName());
    MirroringConfiguration configuration =
        new MirroringConfiguration(testConfiguration, testConfiguration, testConfiguration);
    Connection connection = ConnectionFactory.createConnection(configuration);
    assertTrue(connection instanceof MirroringConnection);
  }
}
