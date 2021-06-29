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
package com.google.cloud.bigtable.test.helper;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper around {@link ServerBuilder} that automatically finds a free port when starting the service.
 *
 * This wraps the call for getting a random free port (e.g. ServerSocket ss = new ServerSocket(0)) into a
 * retry, since it is possible for another process on the host to use the port before the caller of the
 * method can. Though it does not happen often, it happens enough to cause noticeable test flakes. Retrying
 * the call will minimize the flakes.
 */
public class TestServerBuilder {
  private List<BindableService> services = new ArrayList<>();
  private List<ServerServiceDefinition> serverServiceDefinitions = new ArrayList<>();
  private List<ServerInterceptor> interceptors = new ArrayList<>();
  private File certChain = null;
  private File privateKey = null;

  public static TestServerBuilder newInstance() {
    return new TestServerBuilder();
  }

  private TestServerBuilder() {}

  /**
   * See {@link ServerBuilder#addService(BindableService)}.
   */
  public TestServerBuilder addService(BindableService service) {
    services.add(service);
    return this;
  }

  /**
   * See {@link ServerBuilder#addService(ServerServiceDefinition)}.
   */
  public TestServerBuilder addService(ServerServiceDefinition serverServiceDefinition) {
    serverServiceDefinitions.add(serverServiceDefinition);
    return this;
  }

  /**
   * See {@link ServerBuilder#intercept(ServerInterceptor)}.
   */
  public TestServerBuilder intercept(ServerInterceptor interceptor) {
    interceptors.add(interceptor);
    return this;
  }

  /**
   * See {@link ServerBuilder#useTransportSecurity(File, File)}.
   */
  public TestServerBuilder useTransportSecurity(File certChain, File privateKey) {
    this.certChain = certChain;
    this.privateKey = privateKey;
    return this;
  }

  private Server buildServer(int port) {
    ServerBuilder<?> builder = ServerBuilder.forPort(port);

    for (BindableService service : services) {
      builder.addService(service);
    }
    for (ServerServiceDefinition serverServiceDefinition : serverServiceDefinitions) {
      builder.addService(serverServiceDefinition);
    }
    for (ServerInterceptor interceptor : interceptors) {
      builder.intercept(interceptor);
    }

    if (certChain != null || privateKey != null) {
      builder.useTransportSecurity(certChain, privateKey);
    }
    return builder.build();
  }

  /**
   * Gets a free port on the host and starts the server, retrying in case the start fails (notably
   * if the port is no longer available).
   */
  public Server buildAndStart() throws IOException {
    IOException lastError = null;

    int maxRetries = 3;
    for (int retry = 0; retry < maxRetries; retry++) {
      int port;
      try (ServerSocket ss = new ServerSocket(0)) {
        port = ss.getLocalPort();
      }

      Server server = buildServer(port);
      try {
        server.start();
        return server;
      } catch (IOException e) {
        server.shutdown();
        lastError = e;
      }
    }
    throw lastError;
  }
}