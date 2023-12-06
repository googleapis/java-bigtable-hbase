/*
 * Copyright 2023 Google LLC
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
package com.google.cloud.bigtable.hbase_minicluster;

import org.apache.hadoop.hbase.HConstants;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

/**
 * Starts an HBase minicluster in the background an stashes it in the plugin context. It expects
 * that the desired {@code hbase-shaded-testing-util} is injected via {@literal
 * <plugin><dependencies>} at the invocation site.
 */
@Mojo(name = "start", defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST)
public class StartMojo extends AbstractMojo {
  @Parameter(defaultValue = "${project}", readonly = true, required = true)
  private MavenProject project;

  @Parameter(defaultValue = "error")
  private String logLevel;

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    getLog().info("Starting hbase minicluster");

    Controller controller = new Controller();
    int port = controller.start();

    project.getProperties().setProperty(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(port));

    getPluginContext().put(Controller.class, controller);

    getLog().info("minicluster successfully started");
  }
}
