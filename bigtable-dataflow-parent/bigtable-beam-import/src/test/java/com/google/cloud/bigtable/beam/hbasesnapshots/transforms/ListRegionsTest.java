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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.beam.hbasesnapshots.conf.RegionConfig;
import com.google.cloud.bigtable.beam.hbasesnapshots.conf.SnapshotConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Tests the {@link ListRegions} transform to ensure it correctly lists regions from a snapshot
 * manifest.
 */
@RunWith(JUnit4.class)
public class ListRegionsTest {

  private SnapshotConfig snapshotConfig;
  private Configuration conf;
  private Path sourcePath;
  private FileSystem fs;
  private SnapshotManifest manifest;
  private TableDescriptor td;
  private DoFn.OutputReceiver<RegionConfig> receiver;

  @Before
  public void setUp() {
    snapshotConfig = mock(SnapshotConfig.class);
    conf = new Configuration();
    sourcePath = new Path("file:///tmp/data");
    fs = mock(FileSystem.class);
    manifest = mock(SnapshotManifest.class);
    td = mock(TableDescriptor.class);
    receiver = mock(DoFn.OutputReceiver.class);

    when(snapshotConfig.getConfiguration()).thenReturn(conf);
    when(snapshotConfig.getSourcePath()).thenReturn(sourcePath);
    when(snapshotConfig.getSnapshotName()).thenReturn("test-snapshot");
    when(manifest.getTableDescriptor()).thenReturn(td);
  }

  /** Tests that {@link ListRegions.ListRegionsFn} handles snapshots with no regions. */
  @Test
  public void testListRegionsFn_emptySnapshot() throws Exception {
    when(manifest.getRegionManifests()).thenReturn(Collections.emptyList());
    List<RegionInfo> regionInfos = Collections.emptyList();

    try (MockedStatic<TableSnapshotInputFormatImpl> mockedStatic =
        mockStatic(TableSnapshotInputFormatImpl.class)) {
      mockedStatic
          .when(
              () ->
                  TableSnapshotInputFormatImpl.getSnapshotManifest(
                      Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.any()))
          .thenReturn(manifest);
      mockedStatic
          .when(() -> TableSnapshotInputFormatImpl.getRegionInfosFromManifest(manifest))
          .thenReturn(regionInfos);

      ListRegions.ListRegionsFn fn = new ListRegions.ListRegionsFn();
      fn.setup();
      fn.processElement(snapshotConfig, receiver);

      verify(receiver, times(0)).output(Mockito.any());
    }
  }

  /** Tests that {@link ListRegions.ListRegionsFn} correctly lists a single region. */
  @Test
  public void testListRegionsFn_regularSnapshot() throws Exception {
    RegionInfo ri = mock(RegionInfo.class);
    when(ri.getRegionId()).thenReturn(123L);
    when(ri.getStartKey()).thenReturn(new byte[] {1});

    SnapshotProtos.SnapshotRegionManifest regionManifest =
        mock(SnapshotProtos.SnapshotRegionManifest.class);
    HBaseProtos.RegionInfo protoRegionInfo = mock(HBaseProtos.RegionInfo.class);
    when(protoRegionInfo.getRegionId()).thenReturn(123L);
    when(protoRegionInfo.getStartKey()).thenReturn(ByteString.copyFrom(new byte[] {1}));
    when(regionManifest.getRegionInfo()).thenReturn(protoRegionInfo);

    SnapshotProtos.SnapshotRegionManifest.FamilyFiles familyFiles =
        mock(SnapshotProtos.SnapshotRegionManifest.FamilyFiles.class);
    SnapshotProtos.SnapshotRegionManifest.StoreFile storeFile =
        mock(SnapshotProtos.SnapshotRegionManifest.StoreFile.class);
    when(storeFile.getFileSize()).thenReturn(1000L);
    when(familyFiles.getStoreFilesList()).thenReturn(Collections.singletonList(storeFile));
    when(regionManifest.getFamilyFilesList()).thenReturn(Collections.singletonList(familyFiles));

    when(manifest.getRegionManifests()).thenReturn(Collections.singletonList(regionManifest));
    List<RegionInfo> regionInfos = Collections.singletonList(ri);

    try (MockedStatic<TableSnapshotInputFormatImpl> mockedStatic =
        mockStatic(TableSnapshotInputFormatImpl.class)) {
      mockedStatic
          .when(
              () ->
                  TableSnapshotInputFormatImpl.getSnapshotManifest(
                      Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.any()))
          .thenReturn(manifest);
      mockedStatic
          .when(() -> TableSnapshotInputFormatImpl.getRegionInfosFromManifest(manifest))
          .thenReturn(regionInfos);

      ListRegions.ListRegionsFn fn = new ListRegions.ListRegionsFn();
      fn.setup();
      fn.processElement(snapshotConfig, receiver);

      verify(receiver, times(1)).output(Mockito.any(RegionConfig.class));
    }
  }

  /** Tests that {@link ListRegions.ListRegionsFn} correctly lists multiple regions. */
  @Test
  public void testListRegionsFn_multipleRegions() throws Exception {
    RegionInfo ri1 = mock(RegionInfo.class);
    when(ri1.getRegionId()).thenReturn(123L);
    when(ri1.getStartKey()).thenReturn(new byte[] {1});
    RegionInfo ri2 = mock(RegionInfo.class);
    when(ri2.getRegionId()).thenReturn(456L);
    when(ri2.getStartKey()).thenReturn(new byte[] {2});

    SnapshotProtos.SnapshotRegionManifest regionManifest1 =
        mock(SnapshotProtos.SnapshotRegionManifest.class);
    HBaseProtos.RegionInfo protoRegionInfo1 = mock(HBaseProtos.RegionInfo.class);
    when(protoRegionInfo1.getRegionId()).thenReturn(123L);
    when(protoRegionInfo1.getStartKey()).thenReturn(ByteString.copyFrom(new byte[] {1}));
    when(regionManifest1.getRegionInfo()).thenReturn(protoRegionInfo1);

    SnapshotProtos.SnapshotRegionManifest regionManifest2 =
        mock(SnapshotProtos.SnapshotRegionManifest.class);
    HBaseProtos.RegionInfo protoRegionInfo2 = mock(HBaseProtos.RegionInfo.class);
    when(protoRegionInfo2.getRegionId()).thenReturn(456L);
    when(protoRegionInfo2.getStartKey()).thenReturn(ByteString.copyFrom(new byte[] {2}));
    when(regionManifest2.getRegionInfo()).thenReturn(protoRegionInfo2);

    List<SnapshotProtos.SnapshotRegionManifest> manifests = new ArrayList<>();
    manifests.add(regionManifest1);
    manifests.add(regionManifest2);

    when(manifest.getRegionManifests()).thenReturn(manifests);

    List<RegionInfo> regionInfos = new ArrayList<>();
    regionInfos.add(ri1);
    regionInfos.add(ri2);

    try (MockedStatic<TableSnapshotInputFormatImpl> mockedStatic =
        mockStatic(TableSnapshotInputFormatImpl.class)) {
      mockedStatic
          .when(
              () ->
                  TableSnapshotInputFormatImpl.getSnapshotManifest(
                      Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.any()))
          .thenReturn(manifest);
      mockedStatic
          .when(() -> TableSnapshotInputFormatImpl.getRegionInfosFromManifest(manifest))
          .thenReturn(regionInfos);

      ListRegions.ListRegionsFn fn = new ListRegions.ListRegionsFn();
      fn.setup();
      fn.processElement(snapshotConfig, receiver);

      verify(receiver, times(2)).output(Mockito.any(RegionConfig.class));
    }
  }
}
