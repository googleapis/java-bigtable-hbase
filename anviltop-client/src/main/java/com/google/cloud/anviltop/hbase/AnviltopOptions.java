/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.util.Strings;
import com.google.cloud.hadoop.hbase.ChannelOptions;
import com.google.cloud.hadoop.hbase.TransportOptions;
import com.google.common.base.Preconditions;

/**
 * An immutable class providing access to configuration options for Anviltop.
 */
public class AnviltopOptions {

  /**
   * A mutable builder for AnviltopConnectionOptions.
   */
  public static class Builder {
    private String projectId = "";
    private Credential credential;
    private String host;
    private int port;

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setCredential(Credential credential) {
      this.credential = credential;
      return this;
    }

    public Builder setProjectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public AnviltopOptions build() {
      return new AnviltopOptions(host, port, credential, projectId);
    }
  }

  private final String host;
  private final int port;
  private final Credential credential;
  private final String projectId;

  public AnviltopOptions(String host, int port, Credential credential, String projectId) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(host), "Host must not be empty or null.");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId), "ProjectId must not be empty or null.");
    this.host = host;
    this.port = port;
    this.credential = credential;
    this.projectId = projectId;
  }

  public String getProjectId() {
    return projectId;
  }

  public ChannelOptions getChannelOptions() {
    if (this.credential == null) {
      return new ChannelOptions();
    }
    return new ChannelOptions(credential);
  }

  public TransportOptions getTransportOptions() {
    return new TransportOptions(
        TransportOptions.AnviltopTransports.HTTP2_NETTY_TLS,
        host,
        port);
  }
}
