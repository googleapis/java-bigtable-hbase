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

package com.google.cloud.bigtable.beam;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** Collection of utility methods. */
public class TestHelper {

  private static GcsUtil gcsUtil;

  public static void init(String projectId) {
    gcsUtil = initGcsUtil(projectId);
  }

  public static String getTestProperty(String name) {
    return checkNotNull(System.getProperty(name), "Required property missing: " + name);
  }

  public static void cleanUpStorageFolder(String gsPath) throws IOException {
    checkGcsUtilInitialized();
    if (!gsPath.endsWith("/")) {
      gsPath += "/";
    }

    List<String> pathsToRemove = new ArrayList<>();
    for (GcsPath path : gcsUtil.expand(GcsPath.fromUri(gsPath + "*"))) {
      pathsToRemove.add(path.toString());
    }
    pathsToRemove.add(gsPath);
    gcsUtil.remove(pathsToRemove);
  }

  private static GcsUtil initGcsUtil(String projectId) {
    GcpOptions gcpOptions = PipelineOptionsFactory.create().as(GcpOptions.class);
    gcpOptions.setProject(projectId);
    return new GcsUtil.GcsUtilFactory().create(gcpOptions);
  }

  private static void checkGcsUtilInitialized() {
    if (gcsUtil == null) {
      throw new IllegalStateException(
          "GcsUtil has not initialized yet. Invoke TestHelper.init(projectId)");
    }
  }
}
