/*
 * Copyright 2017 Google LLC
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

class MiniClusterEnv extends SharedTestEnv {
  private static final String PORT_KEY = "hbase.zookeeper.property.clientPort";
  private static final Log LOG = LogFactory.getLog(MiniClusterEnv.class);

  @Override
  protected void setup() throws Exception {
    configuration = HBaseConfiguration.create();

    int miniClusterPort = Integer.getInteger(PORT_KEY);
    LOG.info("MiniCluster port: " + miniClusterPort);
    configuration.setInt(PORT_KEY, miniClusterPort);
  }

  @Override
  protected void teardown() throws IOException {}
}
