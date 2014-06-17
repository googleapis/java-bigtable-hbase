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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;

/**
 * Static methods to convert an instance of {@link Configuration}
 * to a {@link AnviltopOptions} instance.
 */
public class AnvilTopOptionsFactory {
  public static final String PROJECT_ID_KEY = "anviltop.project.id";
  public static final String API_ENDPOINT_KEY = "anviltop.endpoint.url";

  public static AnviltopOptions fromConfiguration(Configuration configuration) {
    AnviltopOptions.Builder optionsBuilder = new AnviltopOptions.Builder();

    String projectId = configuration.get(PROJECT_ID_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId),
        String.format("Project ID must be supplied via %s", PROJECT_ID_KEY));
    optionsBuilder.setProjectId(projectId);

    String apiEndpoint = configuration.get(API_ENDPOINT_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(apiEndpoint),
        String.format("API endpoint URL must be supplied via %s", API_ENDPOINT_KEY));
    optionsBuilder.setApiEndpoint(apiEndpoint);

    return optionsBuilder.build();
  }
}
