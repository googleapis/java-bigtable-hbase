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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.resources.PipelineResources;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.GcsStager;
import org.apache.beam.runners.dataflow.util.PackageUtil;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.HashCode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;

public class EnvSetup {
  private final TestProperties properties;
  private static boolean isInitialized = false;

  public static synchronized boolean initialize() {
    if (!isInitialized) {
      new EnvSetup(TestProperties.fromSystem()).run();
      isInitialized = true;
      return true;
    } else {
      return false;
    }
  }

  public EnvSetup(TestProperties properties) {
    this.properties = properties;
  }

  public void run() {
    // Register filesystems
    DataflowPipelineOptions opts = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    opts.setProject(properties.getProjectId());
    opts.setRegion(properties.getDataflowRegion());
    opts.setStagingLocation(properties.getDataflowStagingDir());
    FileSystems.setDefaultPipelineOptions(opts);

    // TODO: stage test/integration-test/{data,hashtable}
    stageJars();
  }

  /**
   * Pre-emptively upload dependencies.
   *
   * <p>This is an optimization to avoid having each of the parallel test threads racing to upload
   * the same files. This implementation is based on
   * https://github.com/apache/beam/blob/master/runners/google-cloud-dataflow-java/src/test/java/org/apache/beam/runners/dataflow/util/GCSUploadMain.java
   */
  private void stageJars() {
    DataflowPipelineOptions opts = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    opts.setProject(properties.getProjectId());
    opts.setRegion(properties.getDataflowRegion());
    opts.setStagingLocation(properties.getDataflowStagingDir());

    List<String> files =
        PipelineResources.detectClassPathResourcesToStage(EnvSetup.class.getClassLoader(), opts)
            .stream()
            .filter(p -> new File(p).exists())
            .collect(Collectors.toList());
    opts.setFilesToStage(files);

    GcsStager.fromOptions(opts)
        .stageFiles(
            opts.getFilesToStage().stream()
                .map(File::new)
                // The real dataflow upload will zip directories. However uploading just the files
                // will handle 90% of the upload, without introducing too much ccomplexity
                .filter(File::isFile)
                .map(
                    file -> {
                      try {
                        HashCode hashCode = Files.asByteSource(file).hash(Hashing.sha256());
                        return PackageUtil.StagedFile.of(
                            file.getAbsolutePath(),
                            hashCode.toString(),
                            Environments.createStagingFileName(file, hashCode));
                      } catch (IOException e) {
                        throw new UncheckedIOException(e);
                      }
                    })
                .collect(Collectors.toList()));
  }
}
