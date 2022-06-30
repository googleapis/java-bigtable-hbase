package com.google.cloud.bigtable.hbase.mirroring.utils.env;

import com.google.cloud.bigtable.hbase.mirroring.utils.TestMismatchDetector;
import com.google.cloud.bigtable.hbase.mirroring.utils.TestWriteErrorConsumer;
import com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;

public class TestEnv {
    public enum Type {
      BIGTABLE, HBASE_MINICLUSTER
    }
    private final TestEnvServers testEnvServers;
    private final Type primaryType;
    private final Type secondaryType;

    TestEnv(TestEnvServers testEnvServers, Type primaryType, Type secondaryType) {
      this.testEnvServers = testEnvServers;
      this.primaryType = primaryType;
      this.secondaryType = secondaryType;

      Preconditions.checkArgument(primaryType != secondaryType);
    }

    public Configuration getMirroringConfig() {
      Configuration configuration = new Configuration(false);

      populateEndpointConfig(configuration, primaryType, MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, MirroringConfigurationHelper.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY);
      populateEndpointConfig(configuration, secondaryType, MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY);


      configuration.set("hbase.client.connection.impl", probeRequiredClassName(
          "com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection",
          "com.google.cloud.bigtable.mirroring.hbase2_x.MirroringConnection"
      ));

      String asynConnectionCls = probeClassName("com.google.cloud.bigtable.mirroring.hbase2_x.MirroringAsyncConnection");
      if (asynConnectionCls != null) {
        configuration.set("hbase.client.async.connection.impl", asynConnectionCls);
      }

      configuration.set(
          "google.bigtable.mirroring.mismatch-detector.factory-impl",
          TestMismatchDetector.Factory.class.getName());

      configuration.set(
          "google.bigtable.mirroring.write-error-consumer.factory-impl",
          TestWriteErrorConsumer.Factory.class.getName());

      return configuration;
    }

    public String getName() {
      return primaryType.name() + "-" + secondaryType.name();
    }

  public Type getPrimaryType() {
    return primaryType;
  }

  public Type getSecondaryType() {
    return secondaryType;
  }

  private static String probeRequiredClassName(String... names) {
    return Objects.requireNonNull(probeClassName(names), "Failed to load any of: " + names);
  }
  private static String probeClassName(String... names) {
    for (String name : names) {
      try {
        Class.forName(name);
        return name;
      } catch (ClassNotFoundException e) {}
    }
    return null;
  }

  private void populateEndpointConfig(Configuration config, Type type, String connectionKey, String prefixKey) {
      switch (type) {
        case HBASE_MINICLUSTER:
          testEnvServers.populateHBase(config, connectionKey, prefixKey);
          break;
        case BIGTABLE:
          testEnvServers.populateBigtable(config, connectionKey, prefixKey);
          break;
        default:
          throw new IllegalArgumentException("Unknown type: " + type);
      }
  }
}
