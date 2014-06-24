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

/**
 * An immutable class providing access to configuration options for Anviltop.
 */
public class AnviltopOptions {

  /**
   * A mutable builder for AnviltopConnectionOptions.
   */
  public static class Builder {
    private String apiEndpoint = "";
    private String projectId = "";

    public Builder setApiEndpoint(String apiEndpoint) {
      this.apiEndpoint = apiEndpoint;
      return this;
    }

    public Builder setProjectId(String projectId) {
      this.projectId = projectId;
      return this;
    }

    public AnviltopOptions build() {
      return new AnviltopOptions(this.apiEndpoint, this.projectId);
    }
  }

  private final String apiEndpoint;
  private final String projectId;

  public AnviltopOptions(String apiEndpoint, String projectId) {
    this.apiEndpoint = apiEndpoint;
    this.projectId = projectId;
  }

  /**
   * The API endpoint to connect to, including version information.
   */
  public String getApiEndpoint() {
    return apiEndpoint;
  }

  /**
   * The project ID that table belong to.
   */
  public String getProjectId() {
    return projectId;
  }
}
