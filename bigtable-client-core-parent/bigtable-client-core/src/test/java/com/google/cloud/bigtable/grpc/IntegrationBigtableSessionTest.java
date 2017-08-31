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
package com.google.cloud.bigtable.grpc;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;

import io.opencensus.contrib.zpages.ZPageHandlers;
import io.opencensus.exporter.trace.stackdriver.StackdriverExporter;

/**
 * Simple connectivity test to test that
 * {@link BigtableSession#createNettyChannel(String, BigtableOptions, io.grpc.ClientInterceptor...)}
 * works correctly outside of the HBase API.
 * @author sduskis
 */
@RunWith(JUnit4.class)
public class IntegrationBigtableSessionTest {
  protected static final Logger LOG = new Logger(IntegrationBigtableSessionTest.class);

  @Test
  public void connectivityTest() {
    try {
      LOG.info("Initializing StackdriverExporter");
      StackdriverExporter.createAndRegister("sduskis-hello-shakespear");
    } catch (Throwable e) {
      LOG.error("Could int register stackdriver", e);
      Throwable t = e.getCause();
      while(t != null) {
        LOG.error("Could int register stackdriver ##", t);
        t = t.getCause();
      }
    }
    try {
      LOG.info("Initializing ZPageHandlers");
      ZPageHandlers.startHttpServerAndRegisterAll(100010);
    } catch (Throwable e) {
      LOG.error("Could int initialize ZPageHandlers", e);
    }
    String property = System.getProperty("bigtableSession-int-test");

    if (!"true".equals(property)) {
      return;
    }
    
    String projectId = System.getProperty("google.bigtable.project.id");
    String instanceId = System.getProperty("google.bigtable.instance.id");

    BigtableOptions options = new BigtableOptions.Builder().setProjectId(projectId)
        .setInstanceId(instanceId).setUserAgent("Test").build();
    try (BigtableSession bs = new BigtableSession(options)) {
      ListTablesRequest request = ListTablesRequest.newBuilder()
          .setParent(options.getInstanceName().getInstanceName()).build();
      bs.getTableAdminClient().listTables(request);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
