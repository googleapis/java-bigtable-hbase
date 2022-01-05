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

import com.google.api.client.util.Joiner;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.grpc.BigtableSession;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableVersionInfo {
  private static final Logger LOG = new Logger(BigtableVersionInfo.class);
  private static final AtomicBoolean wasInitialized = new AtomicBoolean(false);

  // {x-version-update-start:bigtable-client-parent:current}
  public static final String CLIENT_VERSION = "1.26.3-SNAPSHOT";
  // {x-version-update-end}
  public static final String JDK_VERSION = getJavaVersion();

  public static final String CORE_USER_AGENT = "bigtable-" + CLIENT_VERSION + ",jdk-" + JDK_VERSION;

  /** @return The java specification version; for example, 1.7 or 1.8. */
  private static String getJavaVersion() {
    return System.getProperty("java.specification.version");
  }
  /**
   * Gets user agent from bigtable-hbase-version.properties. Returns a default dev user agent with
   * current timestamp if not found.
   */
  public static String getVersion() {
    if (wasInitialized.compareAndSet(false, true)) {
      warnOnVersionConflict();
    }
    return CLIENT_VERSION;
  }

  private static void warnOnVersionConflict() {
    List<URL> classResources;
    // Use BigtableSession as a marker to detect multiple versions on the classpath
    // Convert package.name.class.name -> package/name/class/name.class so that the classfile could
    // be probed as a resource
    String knownClassResourcePath =
        BigtableSession.class.getName().replaceAll("\\.", "/") + ".class";
    try {
      classResources = Collections.list(ClassLoader.getSystemResources(knownClassResourcePath));
    } catch (IOException e) {
      LOG.warn("Failed to probe for client version conflicts", e);
      return;
    }

    if (classResources.size() != 1) {
      LOG.warn(
          "Found multiple copies of the bigtable-client-core on the classpath: "
              + Joiner.on(',').join(classResources));
    }
  }
}
