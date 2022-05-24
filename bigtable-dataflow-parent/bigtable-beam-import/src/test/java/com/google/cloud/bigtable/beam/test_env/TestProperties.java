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

import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.hadoop.conf.Configuration;

public class TestProperties {
  private final String projectId;
  private final String instanceId;
  private final String dataflowRegion;
  private final String workdir;
  private final Optional<String> dataEndpoint;
  private final Optional<String> adminEndpoint;

  public TestProperties(
      String projectId,
      String instanceId,
      String dataflowRegion,
      String workdir,
      Optional<String> dataEndpoint,
      Optional<String> adminEndpoint) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.dataflowRegion = dataflowRegion;
    this.workdir = workdir;
    this.dataEndpoint = dataEndpoint;
    this.adminEndpoint = adminEndpoint;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getDataflowRegion() {
    return dataflowRegion;
  }

  /**
   * Directory that will store ephemeral data that can be re-populated but the test
   *
   * <p>Expected Structure:
   *
   * <ul>
   *   <li>staging - dataflow staged jars
   *   <li>workdir-${uuid} - temporary workdir directory for the a test
   */
  public String getWorkdir() {
    return workdir;
  }

  public String getDataflowStagingDir() {
    return ensureTrailingSlash(getWorkdir()) + "staging/";
  }

  public String getTestWorkdir(UUID testId) {
    return ensureTrailingSlash(getWorkdir()) + "workdir-" + testId + "/";
  }

  public Optional<String> getDataEndpoint() {
    return dataEndpoint;
  }

  public Optional<String> getAdminEndpoint() {
    return adminEndpoint;
  }

  /** Contains persistent fixture data */
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

    return new TestProperties(
        getProp("google.bigtable.project.id"),
        getProp("google.bigtable.instance.id"),
        getProp("region"),
        ensureTrailingSlash(workDir),
        Optional.ofNullable(System.getProperty(BigtableOptionsFactory.BIGTABLE_HOST_KEY)),
        Optional.ofNullable(System.getProperty(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY)));
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

  public void applyTo(Configuration configuration) {
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, getProjectId());
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, getInstanceId());

    dataEndpoint.ifPresent(e -> configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, e));
    adminEndpoint.ifPresent(
        e -> configuration.set(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY, e));
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
