/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.config;

import com.google.api.core.InternalApi;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableVersionInfo {

  private static final Logger LOG = new Logger(BigtableVersionInfo.class);

  public static final String CLIENT_VERSION = getVersion();
  public static final String JDK_VERSION = getJavaVersion();

  public static final String CORE_USER_AGENT = "bigtable-" + CLIENT_VERSION + ",jdk-" + JDK_VERSION;

  /**
   * Gets user agent from bigtable-version.properties. Returns a default dev user agent with current
   * timestamp if not found.
   */
  private static String getVersion() {
    final String defaultVersion = "dev-" + System.currentTimeMillis();
    final String fileName = "bigtable-version.properties";
    final String versionProperty = "bigtable.version";
    try (InputStream stream = BigtableVersionInfo.class.getResourceAsStream(fileName)) {
      if (stream == null) {
        LOG.error("Could not load properties file bigtable-version.properties");
        return defaultVersion;
      }

      Properties properties = new Properties();
      properties.load(stream);
      String value = properties.getProperty(versionProperty);
      if (value == null) {
        LOG.error("%s not found in %s.", versionProperty, fileName);
      } else if (value.startsWith("$")) {
        LOG.info("%s property is not replaced.", versionProperty);
      } else {
        return value;
      }
    } catch (IOException e) {
      LOG.error("Error while trying to get user agent name from %s", e, fileName);
    }
    return defaultVersion;
  }

  /** @return The java specification version; for example, 1.7 or 1.8. */
  private static String getJavaVersion() {
    return System.getProperty("java.specification.version");
  }
}
