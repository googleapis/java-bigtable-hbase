package com.google.cloud.anviltop.hbase;

import com.google.bigtable.anviltop.AnviltopServiceMessages.SampleRowKeysRequest;
import com.google.cloud.anviltop.hbase.adapters.SampledRowKeysAdapter;
import com.google.cloud.hadoop.hbase.AnviltopClient;
import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;

public class AnviltopRegionLocator implements RegionLocator {
  // Reuse the results from previous calls during this time.
  public static long MAX_REGION_AGE_MILLIS = 60 * 1000;

  protected static final Logger LOG = new Logger(AnviltopTable.class);

  private final TableName tableName;
  private final AnviltopOptions options;
  private final AnviltopClient client;
  private final SampledRowKeysAdapter adapter;
  private List<HRegionLocation> regions;
  private long regionsFetchTimeMillis;

  public AnviltopRegionLocator(TableName tableName, AnviltopOptions options, AnviltopClient client) {
    this.tableName = tableName;
    this.options = options;
    this.client = client;
    this.adapter = new SampledRowKeysAdapter(tableName, options.getServerName());
  }

  /**
   * The list of regions will be sorted and cover all the possible rows.
   */
  private synchronized List<HRegionLocation> getRegions(boolean reload) throws IOException {
    // If we don't need to refresh and we have a recent enough version, just use that.
    if (!reload && regions != null &&
        regionsFetchTimeMillis + MAX_REGION_AGE_MILLIS > System.currentTimeMillis()) {
      return regions;
    }

    SampleRowKeysRequest.Builder request = SampleRowKeysRequest.newBuilder();
    request.setProjectId(options.getProjectId());
    request.setTableName(tableName.getQualifierAsString());

    try {
      List<ByteString> rowKeys = client.sampleRowKeys(request.build());
      regions = adapter.adaptResponse(rowKeys);
      regionsFetchTimeMillis = System.currentTimeMillis();
      return regions;
    } catch(ServiceException e) {
      regions = null;
      throw new IOException(e);
    }
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] row) throws IOException {
    return getRegionLocation(row, false);
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] row, boolean reload) throws IOException {
    for(HRegionLocation region : getRegions(reload)) {
      if (region.getRegionInfo().containsRow(row)) {
        return region;
      }
    }
    throw new IOException("Region not found for row: " + Bytes.toStringBinary(row));
  }

  @Override
  public List<HRegionLocation> getAllRegionLocations() throws IOException {
    return getRegions(false);
  }

  @Override
  public byte[][] getStartKeys() throws IOException {
    return getStartEndKeys().getFirst();
  }

  @Override
  public byte[][] getEndKeys() throws IOException {
    return getStartEndKeys().getSecond();
  }

  @Override
  public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
    List<HRegionLocation> regions = getAllRegionLocations();
    byte[][] startKeys = new byte[regions.size()][];
    byte[][] endKeys = new byte[regions.size()][];
    int i = 0;
    for(HRegionLocation region : regions) {
      startKeys[i] = region.getRegionInfo().getStartKey();
      endKeys[i] = region.getRegionInfo().getEndKey();
      i++;
    }
    return Pair.newPair(startKeys, endKeys);
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public void close() throws IOException {
  }
}
