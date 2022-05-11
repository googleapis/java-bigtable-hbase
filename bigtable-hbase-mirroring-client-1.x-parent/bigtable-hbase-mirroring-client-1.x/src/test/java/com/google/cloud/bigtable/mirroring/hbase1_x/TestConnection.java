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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;

public class TestConnection implements Connection {
  public static List<Connection> connectionMocks = new ArrayList<>();
  public static List<Table> tableMocks = new ArrayList<>();
  public static List<ResultScanner> scannerMocks = new ArrayList<>();
  private Connection connectionMock;

  public TestConnection(Configuration conf, boolean managed, ExecutorService pool, User user) {
    connectionMock = mock(Connection.class);
    connectionMocks.add(connectionMock);
  }

  public static void reset() {
    connectionMocks.clear();
    tableMocks.clear();
    scannerMocks.clear();
  }

  @Override
  public Configuration getConfiguration() {
    return connectionMock.getConfiguration();
  }

  @Override
  public Table getTable(TableName tableName) throws IOException {
    ResultScanner scanner = mock(ResultScanner.class);
    doReturn(Result.create(new Cell[0])).when(scanner).next();

    Table table = mock(Table.class);
    doReturn(scanner).when(table).getScanner(any(Scan.class));

    scannerMocks.add(scanner);
    tableMocks.add(table);
    return table;
  }

  @Override
  public Table getTable(TableName tableName, ExecutorService executorService) throws IOException {
    return getTable(tableName);
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
