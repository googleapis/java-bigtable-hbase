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
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * A goal to help developers to run integration tests from within an IDE.
 * The workflow is:
 * <ul>
 *   <li>Start the minicluster:
 *      {@code mvn  -pl bigtable-hbase-1.x-integration-tests hbase-minicluster-maven-plugin:run}
 *      or
 *      {@code mvn  -pl bigtable-hbase-2.x-integration-tests hbase-minicluster-maven-plugin:run}
 *  <li>Grab the {@code hbase.zookeeper.property.clientPort} from the output
 *  <li>Pass it as a system property to the test {@code hbase.zookeeper.property.clientPort=1234}
 *  <li>kill the minicluster by interrupting it with ctrl-c
 */
@Mojo(name = "run")
public class RunMojo extends AbstractMojo {
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info("Starting hbase minicluster");


        Controller controller = new Controller();
        int port = controller.start();

        getLog().info(String.format("%s=%d", HConstants.ZOOKEEPER_CLIENT_PORT, port));
        getLog().info("Running until interrupted");

        try {
            while (!Thread.interrupted()) {
                Thread.sleep(Integer.MAX_VALUE);
            }
        } catch (InterruptedException e) {
        } finally {
            getLog().info("Shutting down minicluster");
            controller.stop();
        }
    }
}
