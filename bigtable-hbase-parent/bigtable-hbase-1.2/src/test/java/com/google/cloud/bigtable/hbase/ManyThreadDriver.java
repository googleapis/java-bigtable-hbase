/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.PickledGraphite;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics;
import com.google.cloud.bigtable.metrics.DropwizardMetricRegistry;
import com.google.cloud.bigtable.metrics.MetricRegistry;
import com.google.cloud.bigtable.metrics.BigtableClientMetrics.MetricLevel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ManyThreadDriver {
  
  static GraphiteReporter reporter = null;

  static {
    String serverIp = System.getProperty("graphite.server.ip");
    if (serverIp != null) {
      MetricRegistry registry = BigtableClientMetrics.getMetricRegistry(MetricLevel.Info);
      DropwizardMetricRegistry dropwizardRegistry;
      if (registry instanceof DropwizardMetricRegistry) {
        dropwizardRegistry = (DropwizardMetricRegistry) registry;
      } else {
        dropwizardRegistry = new DropwizardMetricRegistry();
        BigtableClientMetrics.setMetricRegistry(dropwizardRegistry);
      }

      int port = -1;
      if (serverIp.contains(":")) {
        String[] split = serverIp.split(":");
        serverIp = split[0];
        port = Integer.parseInt(split[1]);
      } else {
        port = Integer.parseInt(System.getProperty("graphite.server.port"));
      }
      System.out.println("Graphite server: " + serverIp + " port: " + port);
      final String prefix = System.getProperty("graphite.prefix");
      final PickledGraphite pickledGraphite =
          new PickledGraphite(new InetSocketAddress(serverIp, port));
      reporter =
          GraphiteReporter.forRegistry(dropwizardRegistry.getRegistry())
              .prefixedWith(prefix)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .filter(MetricFilter.ALL)
              .build(pickledGraphite);
      reporter.start(20, TimeUnit.SECONDS);
      BigtableClientMetrics.setMetricRegistry(registry);
    }
  }

  private static long recordCount;
  private static int valueSize;
  private static int runtimeHours;
  private static int numThreads;

  private static void runTest(String projectId, String instanceId, final String tableName)
      throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
      Admin admin = connection.getAdmin();

      HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
      descriptor.addFamily(new HColumnDescriptor("cf"));
      try {
        admin.createTable(descriptor);
      } catch (IOException ignore) {
        // Soldier on, maybe the table already exists.
      }

      final byte[] value = Bytes.toBytes(RandomStringUtils.randomAlphanumeric(valueSize));

      final long endTimeMs = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(runtimeHours);
      for (int i = 0; i < numThreads; i++) {
        Runnable r =
            new Runnable() {
              @Override
              public void run() {
                try {
                  final Table table = connection.getTable(TableName.valueOf(tableName));

                  while (System.currentTimeMillis() < endTimeMs) {
                    // Workload: two reads and a write.
                    table.get(new Get(Bytes.toBytes(key())));
                    table.get(new Get(Bytes.toBytes(key())));
                    Put p = new Put(Bytes.toBytes(key()));
                    p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col"), value);
                    table.put(p);
                  }
                } catch (Exception e) {
                  System.out.println(e.getMessage());
                }
              }
            };
        executor.execute(r);
      }

      executor.shutdown();
      executor.awaitTermination(runtimeHours, TimeUnit.HOURS);
    } finally {
      executor.shutdownNow();
    }
  }

  private static String key() {
    // TODO Make a parameter?
    return "key-" + String.format("%19d", ThreadLocalRandom.current().nextLong(recordCount));
  }

  public static void main(String[] args) throws Exception {
    // Consult system properties to get project/instance
    // TODO Use standard hbase system properties?
    String projectId = requiredProperty("bigtable.projectID");
    String instanceId = requiredProperty("bigtable.instanceID");
    String table = System.getProperty("bigtable.table", "ManyThreadDriver");
    recordCount = Long.parseLong(System.getProperty("recordCount", "100000"));
    valueSize = Integer.parseInt(System.getProperty("valueSize", "1024"));
    runtimeHours = Integer.parseInt(System.getProperty("runtimeHours", "1"));
    numThreads = Integer.parseInt(System.getProperty("numThreads", "1000"));
    runTest(projectId, instanceId, table);
  }

  private static String requiredProperty(String prop) {
      String value = System.getProperty(prop);
      if (value == null) {
        throw new IllegalArgumentException("Missing required system property: " + prop);
      }
      return value;
  }
}
