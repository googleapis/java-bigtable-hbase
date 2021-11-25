/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.cloud.bigtable.hbase.mirroring.utils.compat.TableCreator;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.rules.ExternalResource;

public class ConnectionRule extends ExternalResource {
  private HBaseMiniClusterSingleton baseMiniCluster;

  public ConnectionRule() {
    if (ConfigurationHelper.isUsingHBaseMiniCluster()) {
      baseMiniCluster = HBaseMiniClusterSingleton.getInstance();
    }
  }

  @Override
  public void before() throws Throwable {
    if (baseMiniCluster != null) {
      baseMiniCluster.start();
    }
  }

  public MirroringConnection createConnection(ExecutorService executorService) throws IOException {
    Configuration configuration = ConfigurationHelper.newConfiguration();
    return createConnection(executorService, configuration);
  }

  public MirroringConnection createConnection(
      ExecutorService executorService, Configuration configuration) throws IOException {
    updateConfigurationWithHbaseMiniClusterProps(configuration);

    Connection conn = ConnectionFactory.createConnection(configuration, executorService);
    return (MirroringConnection) conn;
  }

  public void updateConfigurationWithHbaseMiniClusterProps(Configuration configuration) {
    if (baseMiniCluster != null) {
      baseMiniCluster.updateConfigurationWithHbaseMiniClusterProps(configuration);
    }
  }

  @Override
  protected void after() {
    if (baseMiniCluster != null) {
      baseMiniCluster.stop();
    }
  }

  public String createTableName() {
    return String.format(
        "mirroring-test-table-%s-%s", System.currentTimeMillis(), (new Random()).nextInt());
  }

  public TableName createTable(byte[]... columnFamilies) throws IOException {
    String tableName = createTableName();
    try (MirroringConnection connection = createConnection(Executors.newFixedThreadPool(1))) {
      createTable(connection.getPrimaryConnection(), tableName, columnFamilies);
      createTable(connection.getSecondaryConnection(), tableName, columnFamilies);
      return TableName.valueOf(tableName);
    }
  }

  public TableName createTable(MirroringConnection connection, byte[]... columnFamilies)
      throws IOException {
    String tableName = createTableName();
    createTable(connection.getPrimaryConnection(), tableName, columnFamilies);
    createTable(connection.getSecondaryConnection(), tableName, columnFamilies);
    return TableName.valueOf(tableName);
  }

  public void createTable(Connection connection, String tableName, byte[]... columnFamilies)
      throws IOException {
    try {
      TableCreator tableCreator =
          (TableCreator)
              Class.forName(System.getProperty("integrations.compat.table-creator-impl"))
                  .newInstance();
      tableCreator.createTable(connection, tableName, columnFamilies);
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new IOException(e);
    }
  }
}
