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
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

@Mojo(name = "start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST, requiresProject = true)
public class Start extends AbstractMojo {
  @Parameter( defaultValue = "${project}", readonly = true, required = true )
  private MavenProject project;

  @Parameter( defaultValue = "${project.build.directory}/bigtable-emulator.log", readonly = true, required = true )
  private File logFile;

  @Parameter(readonly = true)
  private File emulatorPath;

  @Parameter(readonly = true, defaultValue = "bigtable.emulator.endpoint")
  private String propertyName;


  public void execute() throws MojoExecutionException {
    if (emulatorPath == null) {
      GcloudHelper helper = new GcloudHelper(Executors.newCachedThreadPool());
      emulatorPath = helper.getEmulatorPath();
    }

    EmulatorController controller = new EmulatorController.Builder()
        .setEmulatorPath(emulatorPath)
        .setLogFile(logFile)
        .build();

    getLog().debug("Starting bigtable emulator");
    try {
      controller.start();
    } catch (IOException e) {
      throw new MojoExecutionException("Failed to start emulator", e);
    }

    // In case the user kills maven using ctrl-c & stop doesn't get to run, make sure to kill the process
    Runtime.getRuntime().addShutdownHook(new EmulatorKiller(controller));

    project.getProperties().setProperty(propertyName, "localhost:" + controller.getPort());
    getLog().info("BIGTABLE_EMULATOR_HOST=localhost:" + controller.getPort());

    setController(controller);
  }

  @SuppressWarnings("unchecked")
  private void setController(EmulatorController controller) {
    getPluginContext().put(EmulatorController.class, controller);
  }


  private static class EmulatorKiller extends Thread {

    private final EmulatorController controller;

    public EmulatorKiller(EmulatorController controller) {
      this.controller = controller;
    }

    @Override
    public void run() {
      if (controller.isStarted()) {
        controller.stop();
      }
    }
  }
}
