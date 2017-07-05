/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

package com.google.cloud.bigtable.test.emulator;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "run")
public class Run extends AbstractMojo {

  @Parameter(readonly = true)
  private File emulatorPath;

  @Parameter(readonly = true, defaultValue = "${project.build.testOutputDirectory}/bigtable-emulator.properties")
  private File propertiesPath;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    if (emulatorPath == null) {
      GcloudHelper helper = new GcloudHelper(Executors.newCachedThreadPool());
      emulatorPath = helper.getEmulatorPath();
    }

    EmulatorController controller = new EmulatorController.Builder()
        .setEmulatorPath(emulatorPath)
        .setPortFilePath(propertiesPath)
        .build();

    getLog().debug("Starting bigtable emulator");
    try {
      controller.start();
    } catch (IOException e) {
      throw new MojoExecutionException("Failed to start emulator", e);
    }
    getLog().info("Bigtable emulator is running on port " + controller.getPort());

    try {
      controller.waitForTermination();
    } catch (InterruptedException e) {
      controller.stop();
      getLog().info("Exiting");
    }
  }
}
