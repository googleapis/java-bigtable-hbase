package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.hbase.Logger;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;


public class SampledRowKeysAdapter {
  protected static final Logger LOG = new Logger(SampledRowKeysAdapter.class);

  private final TableName tableName;
  private final ServerName serverName;

  public SampledRowKeysAdapter(TableName tableName, ServerName serverName) {
    this.tableName = tableName;
    this.serverName = serverName;
  }

  public List<HRegionLocation> adaptResponse(List<SampleRowKeysResponse> responses) {

    List<HRegionLocation> regions = new ArrayList<>();

    // Starting by the first possible row, iterate over the sorted sampled row keys and create regions.
    byte[] startKey = HConstants.EMPTY_START_ROW;

    for (SampleRowKeysResponse response : responses) {
      byte[] endKey = response.getRowKey().toByteArray();

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
