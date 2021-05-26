/*
 * Copyright 2021 Google Inc.
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
package com.google.cloud.bigtable.beam.test_env;

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

@AutoValue
public abstract class TestProperties {
  public abstract String getProjectId();

  public abstract String getInstanceId();

  public abstract String getDataflowRegion();

  /**
   * Directory that will store ephemeral data that can be re-populated but the test
   *
   * <p>Expected Structure:
   *
   * <ul>
   *   <li>staging - dataflow staged jars
   *   <li>workdir-${uuid} - temporary workdir directory for the a test
   */
  public abstract String getWorkdir();

  public String getDataflowStagingDir() {
    return ensureTrailingSlash(getWorkdir()) + "staging/";
  }

  public String getTestWorkdir(UUID testId) {
    return ensureTrailingSlash(getWorkdir()) + "workdir-" + testId + "/";
  }

  // TODO: make this ephemeral
  /** Contains persistent fixture data */
  public abstract String getCloudDataDir();

  public abstract Optional<String> getDataEndpoint();

  public abstract Optional<String> getAdminEndpoint();

  public abstract Optional<Integer> getCellSize();

  public abstract Optional<Integer> getTotalRowCount();

  public abstract Optional<Integer> getPrefixCount();

  public static TestProperties fromSystem() {
    // TODO: once all of the kokoro configs are updated replace this with
    // getProp("dataflow.work-dir")
    String workDirKey = "google.dataflow.work-dir";
    String workDir = System.getProperty(workDirKey);
    // backwards compat for old setup
    if (workDir == null) {
      workDir = System.getProperty("google.dataflow.stagingLocation");
    }
    Preconditions.checkNotNull(workDir, "The system property " + workDirKey + " must be specified");

    return new AutoValue_TestProperties(
        getProp("google.bigtable.project.id"),
        getProp("google.bigtable.instance.id"),
        getProp("region"),
        ensureTrailingSlash(workDir),
        ensureTrailingSlash(getProp("cloud.test.data.folder")),
        Optional.ofNullable(System.getProperty(" google.bigtable.endpoint.host")),
        Optional.ofNullable(System.getProperty("google.bigtable.admin.endpoint.host")),
        Optional.ofNullable(System.getProperty("cell_size")).map(Integer::parseInt),
        Optional.ofNullable(System.getProperty("total_row_count")).map(Integer::parseInt),
        Optional.ofNullable(System.getProperty("prefix_count")).map(Integer::parseInt));
  }

  public void applyTo(DataflowPipelineOptions opts) {
    opts.setRunner(DataflowRunner.class);
    opts.setProject(getProjectId());
    opts.setRegion(getDataflowRegion());
    opts.setStagingLocation(getDataflowStagingDir());
  }

  public void applyTo(GcpOptions opts) {
    opts.setRunner(DataflowRunner.class);
    opts.setProject(getProjectId());
  }

  private static String getProp(String name) {
    String value = System.getProperty(name);
    Preconditions.checkNotNull(value, "The system property " + name + " must be specified");

    return value;
  }

  private static String ensureTrailingSlash(String path) {
    if (path.endsWith("/")) {
      return path;
    }
    return path + "/";
  }
}
