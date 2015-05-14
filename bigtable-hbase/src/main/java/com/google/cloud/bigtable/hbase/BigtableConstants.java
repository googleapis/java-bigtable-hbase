/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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


import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Constants related to Bigtable.
 */
public class BigtableConstants {

  private static final Logger LOG = new Logger(BigtableConstants.class);

  /**
   * Separator between column family and column name for bigtable, as a single byte.
   */
  public static final byte BIGTABLE_COLUMN_SEPARATOR_BYTE = (byte)':';

  /**
   * The length of the column separator, in bytes.
   */
  public static final int BIGTABLE_COLUMN_SEPARATOR_LENGTH = 1;

  /**
   * Byte string of the column family and column name separator for Bigtable.
   */
  public static final ByteString BIGTABLE_COLUMN_SEPARATOR_BYTE_STRING =
      ByteString.copyFrom(new byte[] {BIGTABLE_COLUMN_SEPARATOR_BYTE});

  /**
   * TimeUnit in which HBase clients expects messages to be sent and received.
   */
  public static final TimeUnit HBASE_TIMEUNIT = TimeUnit.MILLISECONDS;

  /**
   * TimeUnit in which Bigtable requires messages to be sent and received.
   */
  public static final TimeUnit BIGTABLE_TIMEUNIT = TimeUnit.MICROSECONDS;

  /**
   * A User-Agent token to be added to User-Agent request header.
   */
  public static final String USER_AGENT = "bigtable-hbase/" + getVersion();

  /**
   * Gets current project version from bigtable-hbase.properties. Returns a default dev version with
   * current timestamp if not found.
   */
  private static String getVersion() {
    final String defaultVersion = "dev-" + System.currentTimeMillis();
    try (InputStream stream =
        BigtableConstants.class.getResourceAsStream("bigtable-hbase.properties")) {
      if (stream == null) {
        LOG.error("Could not load properties file bigtable-hbase.properties");
        return defaultVersion;
      }

      Properties properties = new Properties();
      properties.load(stream);
      String value = properties.getProperty("bigtable.hbase.version");
      if (value == null) {
        LOG.error("bigtable.hbase.version not found in bigtable-hbase.properties.");
      } else if (value.startsWith("$")){
        LOG.info("project.version token is not replaced.");
      } else {
        return value;
      }
    } catch (IOException e) {
      LOG.error(
          String.format("Error while trying to get project.version from bigtable-hbase.properties"),
          e);
    }
    return defaultVersion;
  }
}
