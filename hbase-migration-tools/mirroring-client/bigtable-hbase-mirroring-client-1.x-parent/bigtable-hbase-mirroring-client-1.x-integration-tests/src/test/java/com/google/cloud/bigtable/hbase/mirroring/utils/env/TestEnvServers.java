package com.google.cloud.bigtable.hbase.mirroring.utils.env;

import com.google.cloud.bigtable.emulator.core.EmulatorController;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Connection;

class TestEnvServers {
  private static TestEnvServers instance;

  private int refCount;
  private final EmulatorController bigtableEmulator;
  private final HBaseTestingUtility miniCluster;
  private MiniHBaseCluster miniHBaseCluster;

  public static synchronized TestEnvServers getInstance() {
    if (instance == null) {
      try {
        instance = new TestEnvServers();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return instance;
  }

  public TestEnvServers() throws IOException {
    bigtableEmulator = EmulatorController.createBundled();

    Configuration miniClusterConfig = HBaseConfiguration.create();
    miniClusterConfig.set("hbase.hregion.impl", getFailingRegionName());

    miniCluster = new HBaseTestingUtility(miniClusterConfig);
  }

  private static String getFailingRegionName() {
    String[] failingRegionClasses = new String[]{
        "com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion",
        "org.apache.hadoop.hbase.regionserver.FailingHBaseHRegion2"
    };
    for (String cls : failingRegionClasses) {
      try {
        Class.forName(cls);
        return cls;
      } catch (ClassNotFoundException e) {
      }
    }
    throw new IllegalStateException("Can't find FailingRegion impl");
  }

  public synchronized void retain() throws Exception {
    if (++refCount > 1) {
      return;
    }
    bigtableEmulator.start();
    miniHBaseCluster = miniCluster.startMiniCluster();
  }

  public synchronized void release() throws IOException {
    if (--refCount > 0) {
      return;
    }
    bigtableEmulator.stop();
    miniCluster.shutdownMiniHBaseCluster();
  }

  public void populateBigtable(Configuration mirroringConfig, String connectionKey, String configPrefixKey) {
    mirroringConfig.setClass(connectionKey, BigtableConfiguration.getConnectionClass(), Connection.class);

    Configuration bigtableConfig = BigtableConfiguration.configure("fake-project", "fake-instance");

    String configPrefix = mirroringConfig.get(configPrefixKey);
    if (!Strings.isNullOrEmpty(configPrefix)) {
      configPrefix += ".";
    } else {
      configPrefix = "";
    }

    for (Entry<String, String> e : bigtableConfig) {
      mirroringConfig.set(configPrefix + e.getKey(), e.getValue());
    }
  }

  public void populateHBase(Configuration mirroringConfig, String connectionKey, String configPrefixKey) {
    mirroringConfig.set(connectionKey, "default");


    String configPrefix = mirroringConfig.get(configPrefixKey);
    if (!Strings.isNullOrEmpty(configPrefix)) {
      configPrefix += ".";
    } else {
      configPrefix = "";
    }

    List<String> keys = Arrays.asList("hbase.zookeeper.quorum", "hbase.zookeeper.property.clientPort");
    for (String key : keys) {
      mirroringConfig.set(configPrefix + key, miniCluster.getConfiguration().get(key));
    }
  }
}
