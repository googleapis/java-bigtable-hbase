/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.test_env;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.cloud.bigtable.hbase.Logger;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

class BigtableEnv extends SharedTestEnv {
  private final Logger LOG = new Logger(getClass());

  private static final Set<String> KEYS = Sets.newHashSet(
      "hbase.client.connection.impl",
      "hbase.client.async.connection.impl",
      "hbase.client.registry.impl",
      "google.bigtable.endpoint.port",
      "google.bigtable.endpoint.host",
      "google.bigtable.admin.endpoint.host",
      "google.bigtable.emulator.endpoint.host",
      "google.bigtable.project.id",
      "google.bigtable.instance.id",
      "google.bigtable.use.bulk.api",
      "google.bigtable.use.plaintext.negotiation",
      "google.bigtable.snapshot.cluster.id"
  );

  @Override
  protected void setup() throws IOException {
    configuration = HBaseConfiguration.create();

    String connectionClass = System.getProperty("google.bigtable.connection.impl");
    if (connectionClass != null) {
      configuration.set("hbase.client.connection.impl", connectionClass);
    }

    String asyncConnectionClass = System.getProperty("google.bigtable.async.connection.impl");
    if (asyncConnectionClass != null) {
      configuration.set("hbase.client.async.connection.impl", asyncConnectionClass);
    }

    String registryClass = System.getProperty("google.bigtable.registry.impl");
    if (registryClass != null) {
      configuration.set("hbase.client.registry.impl", registryClass);
    }

    for (Entry<Object, Object> entry : System.getProperties().entrySet()) {
      if (KEYS.contains(entry.getKey())) {
        configuration.set(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    ExecutorService executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("table-deleter").build());
    try (Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin()) {
      for (final TableName tableName : admin
          .listTableNames(Pattern.compile("(test_table|list_table[12]|TestTable).*"))) {
        executor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              admin.deleteTable(tableName);
              LOG.info("Test-setup deleting table: %s", tableName.getNameAsString());
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        });
      }
      executor.shutdown();
      try {
        executor.awaitTermination(10, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  protected void teardown() {
    // noop
  }
}
