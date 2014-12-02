package com.google.cloud.anviltop.hbase.adapters;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

@RunWith(JUnit4.class)
public class TestSampledRowKeysAdapter {
  SampledRowKeysAdapter adapter = new SampledRowKeysAdapter(
      TableName.valueOf("test"),
      ServerName.valueOf("host", 123, 0));

  @Test
  public void testEmptyRowList() {
    List<ByteString> rowKeys = new ArrayList<>();
    List<HRegionLocation> locations = adapter.adaptResponse(rowKeys);
    Assert.assertEquals(1, locations.size());
    HRegionLocation location = locations.get(0);
    Assert.assertArrayEquals(
        HConstants.EMPTY_START_ROW,
        location.getRegionInfo().getStartKey());
    Assert.assertArrayEquals(
        HConstants.EMPTY_END_ROW,
        location.getRegionInfo().getEndKey());

    Assert.assertEquals("host",
        location.getHostname());
    Assert.assertEquals(123,
        location.getPort());
  }

  @Test
  public void testOneRow() {
    byte[] rowKey = Bytes.toBytes("row");
    List<ByteString> rowKeys = new ArrayList<>();
    rowKeys.add(ByteString.copyFrom(rowKey));

    List<HRegionLocation> locations = adapter.adaptResponse(rowKeys);
    Assert.assertEquals(2, locations.size());

    HRegionLocation location = locations.get(0);
    Assert.assertArrayEquals(
        HConstants.EMPTY_START_ROW,
        location.getRegionInfo().getStartKey());
    Assert.assertArrayEquals(
        rowKey,
        location.getRegionInfo().getEndKey());

    location = locations.get(1);
    Assert.assertArrayEquals(
        rowKey,
        location.getRegionInfo().getStartKey());
    Assert.assertArrayEquals(
        HConstants.EMPTY_END_ROW,
        location.getRegionInfo().getEndKey());
  }


  @Test
  public void testEmptyRow() {
    byte[] rowKey = new byte[0];
    List<ByteString> rowKeys = new ArrayList<>();
    rowKeys.add(ByteString.copyFrom(rowKey));

    List<HRegionLocation> locations = adapter.adaptResponse(rowKeys);
    Assert.assertEquals(1, locations.size());
    HRegionLocation location = locations.get(0);
    Assert.assertArrayEquals(
        HConstants.EMPTY_START_ROW,
        location.getRegionInfo().getStartKey());
    Assert.assertArrayEquals(
        HConstants.EMPTY_END_ROW,
        location.getRegionInfo().getEndKey());
  }
}
