/*
 * Copyright 2021 Google LLC
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

import org.apache.hadoop.conf.Configuration;

public class ConfigurationHelper {
  static {
    printDatabasesInfo();
  }

  private static void printDatabasesInfo() {
    String hbaseName =
        ConfigurationHelper.isUsingHBaseMiniCluster() ? "HBase Minicluster" : "HBase";
    String primary = ConfigurationHelper.isPrimaryHBase() ? hbaseName : "Bigtable";
    String secondary = ConfigurationHelper.isSecondaryHBase() ? hbaseName : "Bigtable";

    System.out.printf("Database configuration:\nprimary: %s\nsecondary: %s\n", primary, secondary);
  }

  public static Configuration newConfiguration() {
    Configuration configuration = new Configuration();
    fillDefaults(configuration);
    configuration.addResource(System.getProperty("integration-tests-config-file-name"));
    return configuration;
  }

  public static boolean isUsingHBaseMiniCluster() {
    return Boolean.parseBoolean(newConfiguration().get("use-hbase-mini-cluster"));
  }

  private static boolean isHBaseImplementationClass(String key) {
    // Handles only expected cases to prevent misconfiguration.
    String primaryClass = newConfiguration().get(key);

    if (primaryClass.equals("default")) {
      return true;
    }

    if (primaryClass.contains("google")) {
      return false;
    }

    throw new RuntimeException(String.format("Unexpected connection.impl value: %s", primaryClass));
  }

  public static boolean isPrimaryHBase() {
    return isHBaseImplementationClass("google.bigtable.mirroring.primary-client.connection.impl");
  }

  public static boolean isSecondaryHBase() {
    return isHBaseImplementationClass("google.bigtable.mirroring.secondary-client.connection.impl");
  }

  private static void fillDefaults(Configuration configuration) {
    configuration.setIfUnset(
        "hbase.client.connection.impl",
        "com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection");

    configuration.setIfUnset(
        "google.bigtable.mirroring.mismatch-detector.impl",
        TestMismatchDetector.class.getCanonicalName());

    configuration.setIfUnset(
        "google.bigtable.mirroring.write-error-consumer.impl",
        TestWriteErrorConsumer.class.getCanonicalName());
  }
}
