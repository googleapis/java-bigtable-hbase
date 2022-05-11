/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring.utils;

import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;

/**
 * Keeps count of currently used references and keeps a single Minicluster alive until all
 * references are freed.
 */
public class HBaseMiniClusterSingleton {
  private static HBaseMiniClusterSingleton instance = new HBaseMiniClusterSingleton();
  private static int refCounter = 0;

  private final HBaseTestingUtility helper;

  public HBaseMiniClusterSingleton() {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.hregion.impl", FailingHBaseHRegion.class.getCanonicalName());
    helper = new HBaseTestingUtility(configuration);
  }

  static HBaseMiniClusterSingleton getInstance() {
    return instance;
  }

  public void start() throws Throwable {
    refCounter += 1;
    if (refCounter == 1) {
      helper.startMiniCluster();
    }
  }

  public void updateConfigurationWithHbaseMiniClusterProps(Configuration configuration) {
    String secondaryPrefix = configuration.get("google.bigtable.mirroring.secondary-client.prefix");
    String primaryPrefix = configuration.get("google.bigtable.mirroring.primary-client.prefix");

    String[] keys = new String[] {"hbase.zookeeper.quorum", "hbase.zookeeper.property.clientPort"};
    for (String key : keys) {
      String value = helper.getConfiguration().get(key);
      configuration.set(key, value);
      if (secondaryPrefix != null) {
        configuration.set(String.format("%s.%s", secondaryPrefix, key), value);
      }
      if (primaryPrefix != null) {
        configuration.set(String.format("%s.%s", primaryPrefix, key), value);
      }
    }
  }

  public void stop() {
    refCounter -= 1;
    if (refCounter == 0) {
      try {
        helper.shutdownMiniHBaseCluster();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
