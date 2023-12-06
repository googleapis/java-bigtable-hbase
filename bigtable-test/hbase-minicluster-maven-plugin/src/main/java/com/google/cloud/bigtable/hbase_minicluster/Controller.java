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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.maven.plugin.MojoExecutionException;

/** Simple wrapper around HBase's HBaseTestingUtility. It extends it to configure logging levels. */
class Controller {
  private static final String SIMPLE_LOGGER_LOG_PREFIX = "org.slf4j.simpleLogger.log";
  private static final List<String> NOISY_LOG_HBASE_LOGGERS =
      Collections.unmodifiableList(
          Arrays.asList(
              "org.apache.hadoop", "SecurityLogger.org.apache.hadoop", "BlockStateChange"));

  private HBaseTestingUtility helper;
  private MiniHBaseCluster miniHBaseCluster;

  int start() throws MojoExecutionException {
    // This terribly ugly as it globally changes logging configuration for the entire maven run
    // TODO: fork the jvm with a proper logging config
    for (String logger : NOISY_LOG_HBASE_LOGGERS) {
      System.setProperty(SIMPLE_LOGGER_LOG_PREFIX + "." + logger, "error");
    }

    helper = HBaseTestingUtility.createLocalHTU();
    try {
      miniHBaseCluster = helper.startMiniCluster();
    } catch (Exception e) {

    }

    int port =
        miniHBaseCluster.getConfiguration().getInt("hbase.zookeeper.property.clientPort", -1);

    if (port <= 0) {
      stop();
      throw new MojoExecutionException("Minicluster had invalid port: " + port);
    }
    return port;
  }

  void stop() throws MojoExecutionException {
    if (helper != null) {
      try {
        helper.shutdownMiniCluster();
      } catch (Exception e) {
        throw new MojoExecutionException("Failed to cleanly shutdown minicluster", e);
      }
    }

    helper = null;
  }
}
