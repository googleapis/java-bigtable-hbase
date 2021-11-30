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

import io.opencensus.exporter.trace.zipkin.ZipkinExporterConfiguration;
import io.opencensus.exporter.trace.zipkin.ZipkinTraceExporter;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.config.TraceParams;
import io.opencensus.trace.samplers.Samplers;
import org.junit.rules.ExternalResource;

public class ZipkinTracingRule extends ExternalResource {
  private static int refCount = 0;
  private static boolean shutdown = false;

  @Override
  protected void before() {
    refCount++;
    if (refCount == 1) {
      if (shutdown) {
        throw new IllegalStateException("Tried to register ZipkinTracingRule after it was closed.");
      }
      String zipkinUrl = System.getenv("ZIPKIN_API_URL");
      if (zipkinUrl == null) {
        return;
      }
      System.out.printf("Starting Zipkin tracing at URL: %s\n", zipkinUrl);
      ZipkinTraceExporter.createAndRegister(
          ZipkinExporterConfiguration.builder()
              .setV2Url(String.format("http://%s/api/v2/spans", zipkinUrl))
              .setServiceName("bigtable-hbase-mirroring-client-integration-tests")
              .build());

      TraceConfig traceConfig = Tracing.getTraceConfig();
      TraceParams activeTraceParams = traceConfig.getActiveTraceParams();
      traceConfig.updateActiveTraceParams(
          activeTraceParams.toBuilder().setSampler(Samplers.alwaysSample()).build());
    }
  }

  @Override
  protected void after() {
    refCount--;
    if (refCount == 0) {
      System.out.println("Stopping Zipkin tracing.");
      shutdown = true;
      Tracing.getExportComponent().shutdown();
    }
  }
}
