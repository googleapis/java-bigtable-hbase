package org.apache.hadoop.hbase.client;

import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;

/**
 * Bigtable Impl of {@link AsyncRegistry}}. Hard codes required values, as corresponding Bigtable
 * components do not exist
 * 
 * @author spollapally
 */
public class BigtableAsyncRegistry implements AsyncRegistry {

  public BigtableAsyncRegistry(Configuration conf) {}

  @Override
  public void close() {}

  /**
   * getClusterId() is required for creating and asyncConnection see
   * {@link ConnectionFactory#createAsyncConnection()}
   */
  @Override
  public CompletableFuture<String> getClusterId() {
    return CompletableFuture.completedFuture("TestClusterID");
  }

  @Override
  public CompletableFuture<Integer> getCurrentNrHRS() {
    throw new UnsupportedOperationException("getCurrentNrHRS");
  }

  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    throw new UnsupportedOperationException("getMasterAddress");
  }

  @Override
  public CompletableFuture<Integer> getMasterInfoPort() {
    throw new UnsupportedOperationException("getMasterInfoPort");
  }

  @Override
  public CompletableFuture<RegionLocations> getMetaRegionLocation() {
    throw new UnsupportedOperationException("getMetaRegionLocation");
  }

}
