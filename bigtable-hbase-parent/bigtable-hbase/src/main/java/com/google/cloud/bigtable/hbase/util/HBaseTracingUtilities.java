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
package com.google.cloud.bigtable.hbase.util;

import java.util.Arrays;
import java.util.List;

import com.google.cloud.bigtable.util.TracingUtilities;

import io.opencensus.trace.Tracing;

/**
 * These utilities are to be used in conjunction with the opencensus-contrib-zpages artifact. After
 * including the artifact, the following code can be used for enabling grpcz pages:
 *
 * <pre>
 * int port = ... ; // Choose a port number
 * try {
 *   HBaseTracingUtilities.setupTracingConfig();
 *   ZPageHandlers.startHttpServerAndRegisterAll(port);
 * } catch (Throwable e) {
 *   LOG.error("Could not initialize ZPageHandlers", e);
 * }
 * </pre>
 *
 * This is a method of enabling exports to stackdriver along with the
 * opencensus-exporter-trace-stackdriver artifact:
 *
 * <pre>
 * try {
 *   StackdriverExporter.createAndRegister(projectId);
 * } catch (Throwable e) {
 *   LOG.error("Could not register stackdriver", e);
 * }
 * </pre>
 */
public final class HBaseTracingUtilities {

  /**
   * This is a one time setup for grpcz pages. This adds all of the methods to the Tracing
   * environment required to show a consistent set of methods relating to Cloud Bigtable and HBase
   * methods on the grpcz page.
   */
  public static void setupTracingConfig() {
    TracingUtilities.setupTracingConfig();
    List<String> descriptors = Arrays.asList(
      "BigtableTable.getTableDescriptor",
      "BigtableTable.exists",
      "BigtableTable.existsAll",
      "BigtableTable.batch",
      "BigtableTable.batchCallback",
      "BigtableTable.get",
      "BigtableTable.put",
      "BigtableTable.checkAndPut",
      "BigtableTable.delete",
      "BigtableTable.checkAndDelete",
      "BigtableTable.checkAndMutate",
      "BigtableTable.mutateRow",
      "BigtableTable.append",
      "BigtableTable.increment",
      "BigtableTable.incrementColumnValue"
    );

    Tracing.getExportComponent().getSampledSpanStore().registerSpanNamesForCollection(descriptors);
  }

  private HBaseTracingUtilities() {
  }
}
