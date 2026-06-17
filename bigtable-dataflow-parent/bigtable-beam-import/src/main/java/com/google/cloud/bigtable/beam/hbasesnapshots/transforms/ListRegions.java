/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.transforms;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.SnapshotUtils;
import com.google.cloud.bigtable.beam.hbasesnapshots.coders.RegionConfigCoder;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

/**
 * A {@link PTransform} for listing the regions from snapshot manifest and builds the {@link
 * RegionConfig} instances
 */
@InternalApi("For internal usage only")
public class ListRegions
    extends PTransform<PCollection<SnapshotConfig>, PCollection<RegionConfig>> {

  @VisibleForTesting
  static class ListRegionsFn extends DoFn<SnapshotConfig, RegionConfig> {

    // Beam reuses DoFn instances for multiple elements. Initializing the cache in @Setup
    // ensures that we only create the cache once per DoFn instance lifecycle (per worker thread),
    // avoiding heavy XML parsing overhead for Configuration while also avoiding static state
    // and ensuring thread safety since Beam isolates DoFn instances.
    @Setup
    public void setup() {
      configCache = new HashMap<>();
    }

    private transient Map<Map<String, String>, Configuration> configCache;

    private Map<ByteString, Long> computeRegionSize(SnapshotManifest snapshotManifest) {
      return snapshotManifest.getRegionManifests().stream()
          .collect(
              Collectors.toMap(
                  region -> region.getRegionInfo().getStartKey(),
                  region ->
                      region.getFamilyFilesList().stream()
                          .flatMap(family -> family.getStoreFilesList().stream())
                          .mapToLong(SnapshotProtos.SnapshotRegionManifest.StoreFile::getFileSize)
                          .sum(),
                  (size1, size2) -> size1 + size2));
    }

    /**
     * Reads snapshot file manifest and lists all the regions including the size.
     *
     * @param snapshotConfig - Snapshot Configuration containing source path.
     * @param outputReceiver
     * @throws Exception
     */
    @ProcessElement
    public void processElement(
        @Element SnapshotConfig snapshotConfig, OutputReceiver<RegionConfig> outputReceiver)
        throws Exception {

      Map<String, String> configDetails = snapshotConfig.getConfigurationDetails();
      Configuration configuration =
          configCache.computeIfAbsent(configDetails, SnapshotUtils::getHBaseConfiguration);
      Path sourcePath = snapshotConfig.getSourcePath();
      FileSystem fileSystem = sourcePath.getFileSystem(configuration);
      SnapshotManifest snapshotManifest =
          TableSnapshotInputFormatImpl.getSnapshotManifest(
              configuration, snapshotConfig.getSnapshotName(), sourcePath, fileSystem);

      Map<ByteString, Long> regionsSize = computeRegionSize(snapshotManifest);
      TableDescriptor tableDescriptor = snapshotManifest.getTableDescriptor();

      // Extract region information from the snapshot manifest.
      List<? extends RegionInfo> regionInfos =
          TableSnapshotInputFormatImpl.getRegionInfosFromManifest(snapshotManifest);

      // Emit a RegionConfig for each region in the snapshot.
      regionInfos.stream()
          .map(
              regionInfo ->
                  RegionConfig.builder()
                      .setSnapshotConfig(snapshotConfig)
                      .setTableDescriptor(tableDescriptor)
                      .setRegionInfo(regionInfo)
                      .setRegionSize(
                          regionsSize.getOrDefault(
                              ByteString.copyFrom(regionInfo.getStartKey()), 0L))
                      .build())
          .forEach(outputReceiver::output);
    }
  }

  @Override
  public PCollection<RegionConfig> expand(PCollection<SnapshotConfig> snapshotconfigs) {
    return snapshotconfigs
        .apply("List Regions", ParDo.of(new ListRegionsFn()))
        .setCoder(new RegionConfigCoder())
        .apply(Reshuffle.viaRandomKey());
  }
}
