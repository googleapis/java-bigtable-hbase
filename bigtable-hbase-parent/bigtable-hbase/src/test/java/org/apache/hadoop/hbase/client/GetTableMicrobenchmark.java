/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package org.apache.hadoop.hbase.client;

import static com.google.cloud.bigtable.hbase.BigtableOptionsFactory.*;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;

import com.google.cloud.bigtable.hbase.TestBigtableConnection;

public class GetTableMicrobenchmark {

  private static int COUNT = 1_000_000;

  public static void main(String[] args) throws IOException, InterruptedException {
    Configuration conf = new Configuration(false);
    conf.set(PROJECT_ID_KEY, "projectId");
    conf.set(INSTANCE_ID_KEY, "instanceId");
    int rounds = 100;
    try (final Connection conn = new TestBigtableConnection(conf)) {
      final TableName tableName = TableName.valueOf("foo");
      conn.getTable(tableName);
      {
        for (int j = 0; j < rounds; j++) {
          long start = System.nanoTime();
          for (int i = 0; i < COUNT; i++) {
            conn.getTable(tableName);
          }
          print(start, COUNT);
        }
      }
      {
        System.out.println("======= Concurrent =====");
        ExecutorService es = Executors.newFixedThreadPool(rounds);
        Runnable r = new Runnable() {
          @Override
          public void run() {
            long start = System.nanoTime();
            for (int i = 0; i < COUNT; i++) {
              try {
                conn.getTable(tableName);
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
            print(start, COUNT);
          }
        };
        long start = System.nanoTime();
        for (int j = 0; j < rounds; j++) {
          es.submit(r);
        }
        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
        System.out.println("======= total =====");
        print(start, COUNT * rounds);
      }
    }
  }

  private static void print(long startTimeNanos, int count) {
    long totalTime = System.nanoTime() - startTimeNanos;

    System.out.printf(
        "Got %d in %d ms.  %d nanos/get.  %,f get/sec",
        count, totalTime / 1000000, totalTime / count, count * 1000000000.0 / totalTime);
    System.out.println();
  }

}
