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

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.rules.ExternalResource;

public class ConnectionRule extends ExternalResource {
  private HBaseMiniClusterSingleton baseMiniCluster;

  private boolean shouldUseHBaseMiniCluster() {
    // use-hbase-mini-cluster set to "true" by default to allow for easy running of tests in IDE.
    return Boolean.parseBoolean(System.getProperty("use-hbase-mini-cluster", "true"));
  }

  public ConnectionRule() {
    if (shouldUseHBaseMiniCluster()) {
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
    Configuration configuration = new Configuration();
    return createConnection(executorService, configuration);
  }

  public MirroringConnection createConnection(
      ExecutorService executorService, Configuration configuration) throws IOException {
    fillDefaults(configuration);
    if (baseMiniCluster != null) {
      baseMiniCluster.updateConfigurationWithHbaseMiniClusterProps(configuration);
    }

    Connection conn = ConnectionFactory.createConnection(configuration, executorService);
    return (MirroringConnection) conn;
  }

  private void fillDefaults(Configuration configuration) {
    configuration.setIfUnset(
        "hbase.client.connection.impl",
        "com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection");

    configuration.setIfUnset(
        "google.bigtable.mirroring.mismatch-detector.impl",
        TestMismatchDetector.class.getCanonicalName());

    configuration.setIfUnset(
        "google.bigtable.mirroring.primary-client.connection.impl",
        "com.google.cloud.bigtable.hbase1_x.BigtableConnection");

    // Provide default configuration for Bigtable emulator and HBase minicluster, those values will
    // be used when debugging in IDE.
    configuration.setIfUnset("google.bigtable.mirroring.primary-client.prefix", "default-primary");
    configuration.setIfUnset("default-primary.google.bigtable.project.id", "fake-project");
    configuration.setIfUnset("default-primary.google.bigtable.instance.id", "fake-instance");
    configuration.setIfUnset(
        "default-primary.google.bigtable.emulator.endpoint.host", "localhost:8086");
    configuration.setBooleanIfUnset("default-primary.google.bigtable.use.gcj.client", false);

    configuration.setIfUnset(
        "google.bigtable.mirroring.secondary-client.connection.impl", "default");
    configuration.setIfUnset(
        "google.bigtable.mirroring.secondary-client.prefix", "default-secondary");
    configuration.setIfUnset("default-secondary.zookeeper.recovery.retry", "1");
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

  public TableName createTable(MirroringConnection connection, byte[]... columnFamilies)
      throws IOException {
    String tableName = createTableName();
    createTable(connection.getPrimaryConnection(), tableName, columnFamilies);
    createTable(connection.getSecondaryConnection(), tableName, columnFamilies);
    return TableName.valueOf(tableName);
  }

  public void createTable(Connection connection, String tableName, byte[]... columnFamilies)
      throws IOException {
    Admin admin = connection.getAdmin();

    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
    for (byte[] columnFamilyName : columnFamilies) {
      descriptor.addFamily(new HColumnDescriptor(columnFamilyName));
    }
    admin.createTable(descriptor);
  }
}
