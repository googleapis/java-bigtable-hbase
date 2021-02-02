/*
 * Copyright 2020 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam.validation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Utility class for SyncTable job. */
public class SyncTableUtils {

  private SyncTableUtils() {}

  public static String immutableBytesToString(ImmutableBytesWritable bytes) {
    if (bytes == null) {
      return "";
    }
    return immutableBytesToString(bytes.get());
  }

  public static String immutableBytesToString(byte[] bytes) {
    return Bytes.toStringBinary(bytes);
  }

  /**
   * Creates a HBase configuration for reading HashTable output from GCS bucket located in
   * projectId.
   *
   * @param projectId project containing the GCS bucket holding hashtable output.
   * @param sourceHashDir location of hashtable output from HBase.
   * @return
   */
  public static Configuration createConfiguration(String projectId, String sourceHashDir) {
    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    conf.set("fs.gs.project.id", projectId);
    conf.set("fs.defaultFS", sourceHashDir);
    conf.set("google.cloud.auth.service.account.enable", "true");
    return conf;
  }
}
