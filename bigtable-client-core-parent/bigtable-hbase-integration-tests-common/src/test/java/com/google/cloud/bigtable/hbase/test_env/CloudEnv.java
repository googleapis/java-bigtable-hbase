/*
 * Copyright 2017 Google LLC
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

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

class CloudEnv extends SharedTestEnv {
  private final Logger LOG = new Logger(getClass());

  private static final Set<String> KEYS =
      new HashSet<>(
          Arrays.asList(
              "google.bigtable.endpoint.port",
              "google.bigtable.endpoint.host",
              "google.bigtable.admin.endpoint.host",
              "google.bigtable.emulator.endpoint.host",
              "google.bigtable.use.bulk.api",
              "google.bigtable.use.plaintext.negotiation",
              "google.bigtable.snapshot.cluster.id"));

  @Override
  protected void setup() throws IOException {
    String projectId = System.getProperty("google.bigtable.project.id");
    String instanceId = System.getProperty("google.bigtable.instance.id");

    configuration = HBaseConfiguration.create();
    configuration = BigtableConfiguration.configure(configuration, projectId, instanceId);

    for (Entry<Object, Object> entry : System.getProperties().entrySet()) {
      if (KEYS.contains(entry.getKey())) {
        configuration.set(entry.getKey().toString(), entry.getValue().toString());
      }
    }

    // Auto expire orphaned snapshots. Minimum ttl is 6h
    configuration.setIfUnset(
        "google.bigtable.snapshot.default.ttl.secs",
        String.valueOf(TimeUnit.HOURS.toSeconds(6) + 1));

    // Garbage collect tables that previous runs failed to clean up
    Executor executor = getExecutor();
    try (Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin()) {
      List<CompletableFuture<?>> futures = new ArrayList<>();

      String stalePrefix =
          SharedTestEnvRule.newTimePrefix(
              TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())
                  - TimeUnit.HOURS.toSeconds(6));

      for (final TableName tableName :
          admin.listTableNames(Pattern.compile(SharedTestEnvRule.PREFIX + ".*"))) {

        if (tableName.getNameAsString().compareTo(stalePrefix) > 0) {
          LOG.info("Found fresh table, ignoring: " + tableName);
          continue;
        }

        futures.add(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    admin.deleteTable(tableName);
                    LOG.info("Test-setup deleting table: %s", tableName.getNameAsString());
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                },
                executor));
      }

      try {
        futures.stream().forEach(CompletableFuture::join);
      } catch (Exception e) {
        throw new IOException("Exception while deleting tables", e);
      }
    }
  }

  @Override
  protected void teardown() {
    // noop
  }
}
