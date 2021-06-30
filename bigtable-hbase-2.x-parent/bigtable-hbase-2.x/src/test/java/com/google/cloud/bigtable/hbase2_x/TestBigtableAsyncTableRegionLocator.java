/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase2_x;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestBigtableAsyncTableRegionLocator {

  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  private static final TableName TABLE_NAME = TableName.valueOf("fake-table-name");

  @Mock private DataClientWrapper mockDataClient;

  private BigtableAsyncTableRegionLocator regionLocator;

  @Before
  public void setUp() throws IOException {
    Configuration configuration = new Configuration(false);
    configuration.set(BigtableOptionsFactory.PROJECT_ID_KEY, "fake-project-id");
    configuration.set(BigtableOptionsFactory.INSTANCE_ID_KEY, "fake-instance-id");
    configuration.set(BigtableOptionsFactory.BIGTABLE_HOST_KEY, "localhost");
    configuration.set(BigtableOptionsFactory.BIGTABLE_ADMIN_HOST_KEY, "localhost");
    configuration.set(BigtableOptionsFactory.BIGTABLE_NULL_CREDENTIAL_ENABLE_KEY, "true");
    configuration.set(BigtableOptionsFactory.BIGTABLE_DATA_CHANNEL_COUNT_KEY, "1");
    BigtableHBaseSettings settings = BigtableHBaseSettings.create(configuration);
    regionLocator = new BigtableAsyncTableRegionLocator(TABLE_NAME, settings, mockDataClient);
  }

  @Test
  public void testGetRegionLocation() throws ExecutionException, InterruptedException {
    List<KeyOffset> keyOffsets =
        ImmutableList.of(
            KeyOffset.create(ByteString.copyFromUtf8("a"), 100L),
            KeyOffset.create(ByteString.copyFromUtf8("b"), 100L),
            KeyOffset.create(ByteString.copyFromUtf8("y"), 100L),
            KeyOffset.create(ByteString.copyFromUtf8("z"), 100L));
    when(mockDataClient.sampleRowKeysAsync(TABLE_NAME.getNameAsString()))
        .thenReturn(ApiFutures.immediateFuture(keyOffsets));

    HRegionLocation regionLocationFuture =
        regionLocator.getRegionLocation(Bytes.toBytes("rowKey"), 1, false).get();
    assertEquals("b", Bytes.toString(regionLocationFuture.getRegion().getStartKey()));
    assertEquals("y", Bytes.toString(regionLocationFuture.getRegion().getEndKey()));

    assertEquals(TABLE_NAME, regionLocator.getName());

    regionLocationFuture = regionLocator.getRegionLocation(Bytes.toBytes("1")).get();
    assertEquals("", Bytes.toString(regionLocationFuture.getRegion().getStartKey()));
    assertEquals("a", Bytes.toString(regionLocationFuture.getRegion().getEndKey()));

    regionLocationFuture = regionLocator.getRegionLocation(Bytes.toBytes("a")).get();
    assertEquals("a", Bytes.toString(regionLocationFuture.getRegion().getStartKey()));
    assertEquals("b", Bytes.toString(regionLocationFuture.getRegion().getEndKey()));

    regionLocationFuture = regionLocator.getRegionLocation(Bytes.toBytes("z")).get();
    assertEquals("z", Bytes.toString(regionLocationFuture.getRegion().getStartKey()));
    assertEquals("", Bytes.toString(regionLocationFuture.getRegion().getEndKey()));

    regionLocationFuture = regionLocator.getRegionLocation(Bytes.toBytes("zzz")).get();
    assertEquals("z", Bytes.toString(regionLocationFuture.getRegion().getStartKey()));
    assertEquals("", Bytes.toString(regionLocationFuture.getRegion().getEndKey()));
  }
}
