/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestSampledRowKeysAdapter {
  SampledRowKeysAdapter adapter =
      new SampledRowKeysAdapter(TableName.valueOf("test"), ServerName.valueOf("host", 123, 0)) {
        @Override
        protected HRegionLocation createRegionLocation(byte[] startKey, byte[] endKey) {
          HRegionInfo regionInfo = new HRegionInfo(tableName, startKey, endKey);
          return new HRegionLocation(regionInfo, serverName);
        }
      };

  @Test
  public void testEmptyRowList() {
    List<KeyOffset> rowKeys = new ArrayList<>();
    List<HRegionLocation> locations = adapter.adaptResponse(rowKeys);
    Assert.assertEquals(1, locations.size());
    HRegionLocation location = locations.get(0);
    Assert.assertArrayEquals(HConstants.EMPTY_START_ROW, location.getRegionInfo().getStartKey());
    Assert.assertArrayEquals(HConstants.EMPTY_END_ROW, location.getRegionInfo().getEndKey());

    Assert.assertEquals("host", location.getHostname());
    Assert.assertEquals(123, location.getPort());
  }

  @Test
  public void testOneRow() {
    byte[] rowKey = Bytes.toBytes("row");

    List<KeyOffset> responses = new ArrayList<>();
    responses.add(KeyOffset.create(ByteString.copyFrom(rowKey), 0));

    List<HRegionLocation> locations = adapter.adaptResponse(responses);
    Assert.assertEquals(2, locations.size());

    HRegionLocation location = locations.get(0);
    Assert.assertArrayEquals(HConstants.EMPTY_START_ROW, location.getRegionInfo().getStartKey());
    Assert.assertArrayEquals(rowKey, location.getRegionInfo().getEndKey());

    location = locations.get(1);
    Assert.assertArrayEquals(rowKey, location.getRegionInfo().getStartKey());
    Assert.assertArrayEquals(HConstants.EMPTY_END_ROW, location.getRegionInfo().getEndKey());
  }

  @Test
  public void testEmptyRow() {
    byte[] rowKey = new byte[0];

    List<KeyOffset> responses = new ArrayList<>();
    responses.add(KeyOffset.create(ByteString.copyFrom(rowKey), 0));

    List<HRegionLocation> locations = adapter.adaptResponse(responses);
    Assert.assertEquals(1, locations.size());
    HRegionLocation location = locations.get(0);
    Assert.assertArrayEquals(HConstants.EMPTY_START_ROW, location.getRegionInfo().getStartKey());
    Assert.assertArrayEquals(HConstants.EMPTY_END_ROW, location.getRegionInfo().getEndKey());
  }
}
