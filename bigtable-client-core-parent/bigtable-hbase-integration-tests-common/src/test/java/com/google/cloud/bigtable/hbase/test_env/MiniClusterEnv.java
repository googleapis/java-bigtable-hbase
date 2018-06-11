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
package com.google.cloud.bigtable.hbase.test_env;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;

class MiniClusterEnv extends SharedTestEnv {
  private static final Log LOG = LogFactory.getLog(MiniClusterEnv.class);

  static {
    try {
      HBaseRpcController.class.getName();
    } catch(Throwable t) {
      t.printStackTrace();
    }
  }

  private HBaseTestingUtility helper;

  @Override
  protected void setup() throws Exception {
    LOG.info("Starting hbase minicluster");

    System.setProperty("org.apache.hadoop.hbase.shaded.io.netty.packagePrefix",
      "org.apache.hadoop.hbase.shaded.");

    helper = HBaseTestingUtility.createLocalHTU();
    helper.startMiniCluster();

    // Need to create a separate config for the client to avoid
    // leaking hadoop configs, which messes up local mapreduce jobs
    configuration = HBaseConfiguration.create();

    String[] keys = new String[]{
        "hbase.zookeeper.quorum",
        "hbase.zookeeper.property.clientPort"
    };
    for (String key : keys) {
      configuration.set(key, helper.getConfiguration().get(key));
    }
    LOG.info("Test dir: " + helper.getDataTestDir());
  }

  @Override
  protected void teardown() throws IOException {
    helper.shutdownMiniHBaseCluster();
    System.out.println("Cleaning up testDir: " + helper.getDataTestDir());
    if (!helper.cleanupTestDir()) {
      LOG.warn("Failed to clean up testDir");
    }
    helper = null;
  }
}