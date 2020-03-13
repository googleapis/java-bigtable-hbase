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
package com.google.cloud.bigtable.hbase;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.hbase.util.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The version in this class is project build version specified in the maven. Maven reads the
 * <strong>bigtable-hbase.properties</strong> file present in the resources directory and replaces
 * the property placeholder with project version.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableHBaseVersion {

  private static final Logger LOG = new Logger(BigtableHBaseVersion.class);

  /**
   * Gets user agent from bigtable-hbase-version.properties. Returns a default dev user agent with
   * current timestamp if not found.
   */
  public static String getVersion() {
    final String defaultVersion = "dev-" + System.currentTimeMillis();
    final String fileName = "bigtable-hbase-version.properties";
    final String versionProperty = "bigtable-hbase.version";
    try (InputStream stream = BigtableHBaseVersion.class.getResourceAsStream(fileName)) {
      if (stream == null) {
        LOG.error("Could not load properties file %s", fileName);
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
}
