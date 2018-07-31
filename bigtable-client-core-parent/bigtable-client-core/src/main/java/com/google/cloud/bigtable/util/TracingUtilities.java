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
package com.google.cloud.bigtable.util;

import java.util.ArrayList;
import java.util.List;

import com.google.bigtable.admin.v2.BigtableTableAdminGrpc;
import com.google.bigtable.v2.BigtableGrpc;

import io.grpc.MethodDescriptor;
import io.grpc.ServiceDescriptor;
import io.opencensus.trace.Tracing;

/**
 * These utilities are to be used in conjunction with the opencensus-contrib-zpages artifact to
 * display a consistent set of Cloud Bigtable RPC methods.<p>
 *
 * <b>NOTE: if you are using HBase, use HBaseTracingUtilities instead of TracingUtilities.</b><p>
 *
 * <p> After including the artifact, the following code can be
 * used for enabling grpcz pages:
 *
 * <pre>
 * int port = ... ; // Choose a port number
 * try {
 *   TracingUtilities.setupTracingConfig();
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
public final class TracingUtilities {

  /**
   * This is a one time setup for grpcz pages. This adds all of the methods to the Tracing
   * environment required to show a consistent set of methods relating to Cloud Bigtable on the
   * grpcz page.  If HBase artifacts are present, this will add tracing metadata for HBase methods.
   */
  public static void setupTracingConfig() {
    List<String> descriptors = new ArrayList<>();
    addDescriptor(descriptors, BigtableTableAdminGrpc.getServiceDescriptor());
    addDescriptor(descriptors, BigtableGrpc.getServiceDescriptor());

    Tracing.getExportComponent().getSampledSpanStore().registerSpanNamesForCollection(descriptors);
  }

  /**
   * Reads a list of {@link MethodDescriptor}s from a {@link ServiceDescriptor} and creates a list
   * of Open Census tags.
   *
   * @param descriptors a {@link} of Strings to add Open Census tags to
   * @param serviceDescriptor A {@link ServiceDescriptor} that contains a list of RPCs that are
   *          provided by that service
   */
  private static void addDescriptor(List<String> descriptors, ServiceDescriptor serviceDescriptor) {
    for (MethodDescriptor<?, ?> method : serviceDescriptor.getMethods()) {
      // This is added by Cloud Bigtable's AbstractRetryingOperation
      descriptors.add("Operation." + method.getFullMethodName().replace('/', '.'));
    }
  }

  private TracingUtilities() {
  }
}
