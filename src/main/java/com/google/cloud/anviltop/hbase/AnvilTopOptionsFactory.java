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

  public static final String ANVILTOP_PORT_KEY = "google.anviltop.endpoint.port";
  public static final int DEFAULT_ANVILTOP_PORT = 443;

  public static final String ANVILTOP_HOST_KEY = "google.anviltop.endpoint.host";
  public static final String PROJECT_ID_KEY = "google.anviltop.project.id";

  public static AnviltopOptions fromConfiguration(Configuration configuration) {

    AnviltopOptions.Builder optionsBuilder = new AnviltopOptions.Builder();

    String projectId = configuration.get(PROJECT_ID_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(projectId),
        String.format("Project ID must be supplied via %s", PROJECT_ID_KEY));
    optionsBuilder.setProjectId(projectId);

    String host = configuration.get(ANVILTOP_HOST_KEY);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(host),
        String.format("API endpoint host must be supplied via %s", ANVILTOP_HOST_KEY));
    optionsBuilder.setHost(host);

    int port = configuration.getInt(ANVILTOP_PORT_KEY, DEFAULT_ANVILTOP_PORT);
    optionsBuilder.setPort(port);

    return optionsBuilder.build();
  }
}
