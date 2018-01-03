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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.shaded.org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import io.grpc.internal.GrpcUtil;

public class RecreateConnectionDriver {
  
  static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");

  private static long recordCount;
  private static int valueSize;
  private static int runtimeHours;
  private static int numThreads;
  private static int numQualifiers;

  private static void runTest(
      String projectId, String instanceId, final String tableNameStr)
      throws Exception {
    byte[][] qualifiers = generateQualifiers(numQualifiers);
    final TableName tableName = TableName.valueOf(tableNameStr);
    final AtomicBoolean finished = new AtomicBoolean(false);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads,
      GrpcUtil.getThreadFactory("WORK_EXECUTOR-%d", true));
    ScheduledExecutorService finishExecutor = setupShutdown(finished);
    try (Connection connection = connect(projectId, instanceId)) {
      setupTable(tableName, connection);
    }

    Runnable setFinish = new Runnable(){
      @Override
      public void run() {
        finished.set(true);
      }
    };

    for(int i=0;i<10;i++) {
      try (Connection connection = connect(projectId, instanceId)) {
        finished.set(false);
        finishExecutor.schedule(setFinish, 10, TimeUnit.SECONDS);
        createWorker(connection, tableName, finished, qualifiers).run();;
      }
      executor.shutdown();
      executor.awaitTermination(runtimeHours, TimeUnit.HOURS);
      // Sleep 10 seconds to allow stragglers to finish.
      Thread.sleep(10000);
    }
    executor.shutdownNow();
    finishExecutor.shutdownNow();
  }

  protected static Connection connect(String projectId, String instanceId) {
    Configuration conf = BigtableConfiguration.configure(projectId, instanceId);
    conf.set(BigtableOptionsFactory.BIGTABLE_USE_TIMEOUTS_KEY, "true");
    conf.set(BigtableOptionsFactory.BIGTABLE_RPC_TIMEOUT_MS_KEY, "2000");
    conf.set(BigtableOptionsFactory.MAX_ELAPSED_BACKOFF_MILLIS_KEY, "2000");
    return BigtableConfiguration.connect(conf);
  }

  static void setupTable(final TableName tableName, Connection connection) throws IOException {
    try(Admin admin = connection.getAdmin()) {
      TableDescriptorBuilder descriptor = TableDescriptorBuilder.newBuilder(tableName);
      descriptor.addColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY));
      try {
        admin.createTable(descriptor.build());
        System.out.println("Created the table");
      } catch (IOException ignore) {
        // Soldier on, maybe the table already exists.
      }
  
      try {
        System.out.println("Truncating the table");
        admin.truncateTable(tableName, false);
      } catch (IOException ignore) {
        // Soldier on.
      }
    }
  }

  static ScheduledExecutorService setupShutdown(final AtomicBoolean finished) {
    ScheduledExecutorService finishExecutor =
        Executors.newScheduledThreadPool(1, GrpcUtil.getThreadFactory("FINISH_SCHEDULER-%d", true));
    finishExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        finished.set(true);
      }
    }, runtimeHours, TimeUnit.HOURS);
    return finishExecutor;
  }

  static Runnable createWorker(
      final Connection connection,
      TableName tableName,
      final AtomicBoolean finished,
      final byte[][] qualifiers)
      throws IOException {
    final Table table = connection.getTable(tableName);
    final byte[][] values = new byte[qualifiers.length][];
    for (int i = 0; i < qualifiers.length; i++) {
      values[i] = Bytes.toBytes(RandomStringUtils.randomAlphanumeric(valueSize / values.length));
    }
    return new Runnable() {
      @Override
      public void run() {
        while (!finished.get()) {
          try {
            // Workload: two reads and a write.
            final byte[] key = Bytes.toBytes(key());
            table.get(new Get(key));
            table.get(new Get(key));
            Put p = new Put(key);
            for (int i = 0; i < qualifiers.length; i++) {
              p.addColumn(COLUMN_FAMILY, qualifiers[i], values[i]);
            }
            table.put(p);
          } catch(Throwable t) {
            t.printStackTrace();
          }
        }
      }
    };
  }

  private static byte[][] generateQualifiers(int qualifierCount) {
    final byte[][] qualifiers = new byte[qualifierCount][];
    for (int i = 0; i < qualifierCount; i++) {
      qualifiers[i] = Bytes.toBytes("qualifier_" + i);
    }
    return qualifiers;
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
    numQualifiers = Integer.parseInt(System.getProperty("numQualifiers", "20"));
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
