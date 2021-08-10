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

public class MirroringConnection implements Connection {
  private MirroringConfiguration configuration;
  private Connection primaryConnection;
  private Connection secondaryConnection;

  /**
   * The constructor called from {@link
   * org.apache.hadoop.hbase.client.ConnectionFactory#createConnection(Configuration)} and in its
   * many forms via reflection with this specific signature.
   *
   * <p>Parameters are passed down to ConnectionFactory#createConnection method, connection errors
   * are passed back to the user.
   */
  public MirroringConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws IOException {
    assert !managed; // This is always-false legacy hbase parameter.
    this.configuration = new MirroringConfiguration(conf);
    this.primaryConnection =
        ConnectionFactory.createConnection(this.configuration.primaryConfiguration, pool, user);
    this.secondaryConnection =
        ConnectionFactory.createConnection(this.configuration.secondaryConfiguration, pool, user);
  }

  @Override
  public Configuration getConfiguration() {
    return this.configuration;
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
