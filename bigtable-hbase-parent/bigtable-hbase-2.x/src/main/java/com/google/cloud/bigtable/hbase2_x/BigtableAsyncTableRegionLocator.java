package com.google.cloud.bigtable.hbase2_x;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncTableRegionLocator;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.Logger;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.hbase.adapters.SampledRowKeysAdapter;

/**
 * @author spollapally
 */
public class BigtableAsyncTableRegionLocator implements AsyncTableRegionLocator {
  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(BigtableAsyncTableRegionLocator.class);

  private final TableName tableName;
  private final BigtableDataClient client;
  private final SampledRowKeysAdapter adapter;
  private final BigtableTableName bigtableTableName;
  private List<HRegionLocation> regions;

  public BigtableAsyncTableRegionLocator(TableName tableName, BigtableOptions options,
      BigtableDataClient client) {
    this.tableName = tableName;
    this.client = client;
    this.bigtableTableName = options.getInstanceName().toTableName(tableName.getNameAsString());
    ServerName serverName = ServerName.valueOf(options.getDataHost(), options.getPort(), 0);
    this.adapter = new SampledRowKeysAdapter(tableName, serverName) {

      @Override
      protected HRegionLocation createHRegionLocation(HRegionInfo hRegionInfo,
          ServerName serverName) {
        return new HRegionLocation(hRegionInfo, serverName);
      }
    };
  }

  @Override
  public TableName getName() {
    return this.tableName;
  }

  @Override
  public CompletableFuture<HRegionLocation> getRegionLocation(byte[] arg0, boolean arg1) {
    SampleRowKeysRequest.Builder request = SampleRowKeysRequest.newBuilder();
    request.setTableName(bigtableTableName.toString());
    LOG.debug("Sampling rowkeys for table %s", request.getTableName());

    FutureUtils.toCompletableFuture(client.sampleRowKeysAsync(request.build()))
        .thenApplyAsync((responses) -> adapter.adaptResponse(responses));
    //thenApplyAsync(adapter.adaptResponse(responses))
    
    return null;
  }
}
