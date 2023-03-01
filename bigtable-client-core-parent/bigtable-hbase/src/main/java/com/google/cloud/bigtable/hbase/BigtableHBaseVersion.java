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
import com.google.common.base.Joiner;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;

/** For internal use only - public for technical reasons. */
@InternalApi("For internal usage only")
public class BigtableHBaseVersion {
  private static final Log LOG = LogFactory.getLog(BigtableHBaseVersion.class);

  private static final AtomicBoolean wasInitialized = new AtomicBoolean(false);

  // {x-version-update-start:bigtable-client-parent:current}
  public static final String VERSION = "2.7.3-SNAPSHOT";
  // {x-version-update-end}

  /**
   * Gets user agent from bigtable-hbase-version.properties. Returns a default dev user agent with
   * current timestamp if not found.
   */
  public static String getVersion() {
    if (wasInitialized.compareAndSet(false, true)) {
      warnOnVersionConflict();
    }
    return VERSION;
  }

  private static void warnOnVersionConflict() {
    List<URL> classResources;
    // Use AbstractBigtableConnection as a marker to detect multiple versions on the classpath
    // Convert package.name.class.name -> package/name/class/name.class so that the classfile could
    // be probed as a resource
    String knownClassResourcePath =
        AbstractBigtableConnection.class.getName().replaceAll("\\.", "/") + ".class";
    try {
      classResources = Collections.list(ClassLoader.getSystemResources(knownClassResourcePath));
    } catch (IOException e) {
      LOG.warn("Failed to probe for client version conflicts", e);
      return;
    }

    if (classResources.size() != 1) {
      LOG.warn(
          "Found multiple copies of the bigtable-hbase on the classpath: "
              + Joiner.on(',').join(classResources));
    }
  }
}
