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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringMetricsViews;
import io.opencensus.exporter.stats.prometheus.PrometheusStatsCollector;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import org.junit.rules.ExternalResource;

public class PrometheusStatsCollectionRule extends ExternalResource {
  private static int refCount = 0;
  private static boolean shutdown = false;
  private static HTTPServer server;

  @Override
  protected void before() throws IOException {
    refCount++;
    if (refCount == 1) {
      if (shutdown) {
        throw new IllegalStateException(
            "Tried to register PrometheusStatsCollectionRule after it was closed.");
      }
      String serverPort = System.getenv("PROMETHEUS_SERVER_PORT");
      if (serverPort == null) {
        return;
      }

      System.out.printf("Starting Prometheus server at port: %s\n", serverPort);
      PrometheusStatsCollector.createAndRegister();
      server = new HTTPServer("localhost", Integer.parseInt(serverPort), false);

      MirroringMetricsViews.registerMirroringClientViews();
    }
  }

  @Override
  protected void after() {
    refCount--;
    if (refCount == 0) {
      System.out.println("Stopping Prometheus server.");
      shutdown = true;
      if (server != null) {
        server.stop();
      }
    }
  }
}
