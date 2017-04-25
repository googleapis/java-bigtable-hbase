/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import com.google.auth.Credentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.Logger;
import com.google.common.base.Strings;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AbstractBigtableConnection;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * A simple utility class for checking and displaying common Bigtable-HBase configuration values.
 *
 * Expected usage: hbase com.google.cloud.bigtable.hbase.CheckConfig
 *
 * @author sduskis
 * @version $Id: $Id
 */
@SuppressWarnings("deprecation")
public class CheckConfig {
  /**
   * <p>main.</p>
   *
   * @param args an array of {@link java.lang.String} objects.
   * @throws java.io.IOException if any.
   * @throws java.security.GeneralSecurityException if any.
   */
  public static void main(String[] args) throws IOException, GeneralSecurityException {
    Logger logger = new Logger(CheckConfig.class);
    GenericOptionsParser optionsParser =
        new GenericOptionsParser(HBaseConfiguration.create(), args);
    Configuration fullConfiguration = optionsParser.getConfiguration();

    BigtableOptions options;
    try {
      options = BigtableOptionsFactory.fromConfiguration(fullConfiguration);
    } catch (IOException | RuntimeException exc) {
      logger.warn("Encountered errors attempting to parse configuration.", exc);
      return;
    }

    System.out.println(String.format("User Agent: %s", options.getUserAgent()));
    System.out.println(String.format("Project ID: %s", options.getProjectId()));
    System.out.println(String.format("Instance Id: %s", options.getInstanceId()));
    System.out.println(String.format("Instance admin host: %s", options.getInstanceAdminHost()));
    System.out.println(String.format("Table admin host: %s", options.getTableAdminHost()));
    System.out.println(String.format("Data host: %s", options.getDataHost()));

    Credentials credentials = CredentialFactory.getCredentials(options.getCredentialOptions());
    try {
      System.out.println("Attempting credential refresh...");
      credentials.refresh();
    } catch (IOException ioe) {
      logger.warn("Encountered errors attempting to refresh credentials.", ioe);
      return;
    }

    String configuredConnectionClass =
        fullConfiguration.get(HConnection.HBASE_CLIENT_CONNECTION_IMPL);

    boolean isCorrectClassSpecified = false;

    if (!Strings.isNullOrEmpty(configuredConnectionClass)) {
      try {
        Class<?> connectionClass = Class.forName(configuredConnectionClass);
        isCorrectClassSpecified =
            AbstractBigtableConnection.class.isAssignableFrom(connectionClass);
      } catch (Exception e) {
        // Ignore. Problems will be logged in the println below.
      }
    }
    // We can actually determine if this value is correct (disregarding custom subclasses).
    System.out.println(
        String.format(
            "HBase Connection Class = %s %s",
            configuredConnectionClass,
            isCorrectClassSpecified ? "(OK)" : "(Configuration error)"));

    System.out.println("Opening table admin connection...");
    try (Connection conn = ConnectionFactory.createConnection(fullConfiguration)) {
      try (Admin admin = conn.getAdmin()) {
        System.out.println(String.format("Tables in cluster %s:", options.getInstanceId()));
        TableName[] tableNames = admin.listTableNames();
        if (tableNames.length == 0) {
          System.out.println("No tables found.");
        } else {
          for (TableName table : tableNames) {
            System.out.println(table.getNameAsString());
          }
        }
      }
      System.out.println("Closing connection...");
    }
  }
}
