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
package com.google.cloud.bigtable;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.hbase.wrappers.AdminClientWrapper;
import com.google.cloud.bigtable.hbase.wrappers.BigtableApi;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import com.google.cloud.bigtable.hbase.wrappers.DataClientWrapper;
import com.google.cloud.bigtable.hbase2_x.BigtableAsyncAdmin;
import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.Size.Unit;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.CommonConnection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class BigtableAsyncAdminTest {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock CommonConnection connection;
  @Mock BigtableHBaseSettings settings;
  @Mock BigtableApi bigtableApi;
  @Mock AdminClientWrapper adminClientWrapper;
  @Mock DataClientWrapper dataClientWrapper;

  private BigtableAsyncAdmin admin;

  @Before
  public void setUp() throws Exception {
    when(connection.getBigtableSettings()).thenReturn(settings);
    when(connection.getBigtableApi()).thenReturn(bigtableApi);
    when(bigtableApi.getAdminClient()).thenReturn(adminClientWrapper);
    when(bigtableApi.getDataClient()).thenReturn(dataClientWrapper);
    when(connection.getDisabledTables()).thenReturn(new HashSet<>());
    when(connection.getConfiguration()).thenReturn(new Configuration(false));

    admin = BigtableAsyncAdmin.createInstance(connection);
  }

  @Test
  public void testRegionMetricsFileSize() throws ExecutionException, InterruptedException {
    String tableId = "fake-table";
    SettableApiFuture<List<KeyOffset>> resultFuture = SettableApiFuture.create();
    resultFuture.set(
        Arrays.asList(
            KeyOffset.create(ByteString.copyFromUtf8("a"), 100),
            KeyOffset.create(ByteString.copyFromUtf8("b"), 250),
            KeyOffset.create(ByteString.copyFromUtf8(""), 401)));
    when(dataClientWrapper.sampleRowKeysAsync(tableId)).thenReturn(resultFuture);
    List<RegionMetrics> results =
        admin
            .getRegionMetrics(ServerName.valueOf("host", 123, 0), TableName.valueOf("fake-table"))
            .get();
    assertThat(results).hasSize(3);
    assertThat(results.get(0).getStoreFileSize()).isEqualTo(new Size(100, Unit.BYTE));
    assertThat(results.get(1).getStoreFileSize()).isEqualTo(new Size(150, Unit.BYTE));
    assertThat(results.get(2).getStoreFileSize()).isEqualTo(new Size(151, Unit.BYTE));
  }
}
