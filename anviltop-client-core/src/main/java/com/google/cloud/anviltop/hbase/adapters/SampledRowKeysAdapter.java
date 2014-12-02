package com.google.cloud.anviltop.hbase.adapters;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;


public class SampledRowKeysAdapter {
  private final TableName tableName;
  private final ServerName serverName;

  public SampledRowKeysAdapter(TableName tableName, ServerName serverName) {
    this.tableName = tableName;
    this.serverName = serverName;
  }

  public List<HRegionLocation> adaptResponse(List<ByteString> rowKeys) {
    List<HRegionLocation> regions = new ArrayList<>();

    // Starting by the first possible row, iterate over the sorted sampled row keys and create regions.
    byte[] startKey = HConstants.EMPTY_START_ROW;

    for (ByteString rowKey : rowKeys) {
      byte[] endKey = rowKey.toByteArray();

      // Avoid empty regions.
      if (Bytes.equals(startKey, endKey)) {
        continue;
      }
      HRegionInfo regionInfo = new HRegionInfo(tableName, startKey, endKey);
      startKey = endKey;

      regions.add(new HRegionLocation(regionInfo, serverName));
    }

    // Create one last region if the last region doesn't reach the end or there are no regions.
    byte[] endKey = HConstants.EMPTY_END_ROW;
    if (regions.isEmpty() || !Bytes.equals(startKey, endKey)) {
      HRegionInfo regionInfo = new HRegionInfo(tableName, startKey, endKey);
      regions.add(new HRegionLocation(regionInfo, serverName));
    }
    return regions;
  }
}
