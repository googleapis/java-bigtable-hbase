/*
 * Copyright 2024 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots;

import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.MimeTypes;

/** Contains various helper methods to handle different tasks while executing tests. */
public class SnapshotTestHelper {
  private SnapshotTestHelper() {}

  /**
   * Helper to generate files for testing.
   *
   * @param filePath The path to the file to write.
   * @param fileContents The content to write.
   * @throws IOException If an error occurs while creating or writing the file.
   */
  static void writeToFile(String filePath, String fileContents) throws IOException {

    ResourceId resourceId = FileSystems.matchNewResource(filePath, false);

    // Write the file contents to the channel and close.
    try (ReadableByteChannel readChannel =
        Channels.newChannel(new ByteArrayInputStream(fileContents.getBytes()))) {
      try (WritableByteChannel writeChannel = FileSystems.create(resourceId, MimeTypes.TEXT)) {
        ByteStreams.copy(readChannel, writeChannel);
      }
    }
  }

  /**
   * @param restorePath - Path to which snapshots will be restored temporarily
   * @return SnapshotConfig - Returns the snapshot config
   */
  public static SnapshotConfig newSnapshotConfig(String restorePath) {
    return newSnapshotConfig("testsourcepath", restorePath);
  }

  public static SnapshotConfig newSnapshotConfig(String sourcePath, String restorePath) {
    return SnapshotConfig.builder()
        .setProjectId("testproject")
        .setSourceLocation(sourcePath)
        .setRestoreLocation(restorePath)
        .setSnapshotName("testsnapshot")
        .setTableName("testtable")
        .setConfigurationDetails(new HashMap<String, String>())
        .build();
  }

  /**
   * Helper method providing pipeline options.
   *
   * @param args list of pipeline arguments.
   */
  static ImportJobFromHbaseSnapshot.ImportOptions getPipelineOptions(String[] args) {
    return PipelineOptionsFactory.fromArgs(args).as(ImportJobFromHbaseSnapshot.ImportOptions.class);
  }

  /**
   * Creates Fake Storage Objects
   *
   * @param basePath File System base path
   * @param objectNames List of object names
   * @return List of matching Storage objects
   */
  static List<StorageObject> createFakeStorageObjects(String basePath, List<String> objectNames) {
    if (objectNames == null) return null;

    List<StorageObject> storageObjects = new ArrayList<>();
    objectNames.forEach(
        name -> {
          StorageObject object = new StorageObject();
          object.setId(Joiner.on("/").join(basePath, ".hbase-snapshot", name, ".snapshotinfo"));
          storageObjects.add(object);
        });

    return storageObjects;
  }

  static Map<String, String> buildMapFromList(String[] values) {
    if (values.length % 2 != 0)
      throw new IllegalArgumentException(
          "Input should contain even number of values to represent both"
              + " key and value for the map.");
    Map<String, String> data = new HashMap<>();
    for (int i = 0; i < values.length; i += 2) data.put(values[i], values[i + 1]);
    return data;
  }
}
