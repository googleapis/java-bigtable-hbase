package com.google.cloud.bigtable.beam.hbasesnapshots.transforms;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.beam.hbasesnapshots.coders.RegionConfigCoder;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} for listing the regions from snapshot manifest and builds the {@link
 * RegionConfig} instances
 */
@InternalApi("For internal usage only")
public class ListRegions
    extends PTransform<PCollection<SnapshotConfig>, PCollection<RegionConfig>> {

  @VisibleForTesting
  static class ListRegionsFn extends DoFn<SnapshotConfig, RegionConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(ListRegionsFn.class);

    private static long GIGA_BYTE = 1024 * 1024 * 1024;

    private Map<Long, Long> computeRegionSize(SnapshotManifest snapshotManifest) {
      Map<Long, Long> regionsSize = new HashMap<>();
      long totalSize = 0;
      for (SnapshotProtos.SnapshotRegionManifest regionManifest :
          snapshotManifest.getRegionManifests()) {
        totalSize = 0;
        for (SnapshotProtos.SnapshotRegionManifest.FamilyFiles familyFiles :
            regionManifest.getFamilyFilesList()) {
          for (SnapshotProtos.SnapshotRegionManifest.StoreFile StoreFile :
              familyFiles.getStoreFilesList()) totalSize += StoreFile.getFileSize();
        }
        regionsSize.put(regionManifest.getRegionInfo().getRegionId(), totalSize);
      }

      return regionsSize; // (int)Math.ceil((totalSize * 1.0)/GIGA_BYTE);
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

      Configuration configuration = snapshotConfig.getConfiguration();
      Path sourcePath = snapshotConfig.getSourcePath();
      FileSystem fileSystem = sourcePath.getFileSystem(configuration);
      SnapshotManifest snapshotManifest =
          TableSnapshotInputFormatImpl.getSnapshotManifest(
              configuration, snapshotConfig.getSnapshotName(), sourcePath, fileSystem);

      Map<Long, Long> regionsSize = computeRegionSize(snapshotManifest);
      TableDescriptor tableDescriptor = snapshotManifest.getTableDescriptor();

      // Read Region info
      List<? extends RegionInfo> regionInfos =
          TableSnapshotInputFormatImpl.getRegionInfosFromManifest(snapshotManifest);

      // List the regions
      regionInfos.stream()
          .map(
              regionInfo ->
                  RegionConfig.builder()
                      .setSnapshotConfig(snapshotConfig)
                      .setTableDescriptor(tableDescriptor)
                      .setRegionInfo(regionInfo)
                      .setRegionSize(regionsSize.get(regionInfo.getRegionId()))
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
