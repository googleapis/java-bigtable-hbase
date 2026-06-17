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
package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import com.google.api.core.InternalApi;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A workalike for {@link org.apache.hadoop.hbase.client.ClientSideRegionScanner}.
 *
 * <p>It serves the same purpose, but skips block and mobFile cache initialization. Those caches
 * dont appear to useful for the import job and leak threads on shutdown
 */
@InternalApi("For internal usage only")
public class HBaseRegionScanner implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseRegionScanner.class);

  private static final String DEFAULT_HFILE_BLOCK_CACHE_POLICY = "IndexOnlyLRU";
  private static final long DEFAULT_HFILE_BLOCK_CACHE_SIZE = 33554432L; // 32 MB
  private static final int DEFAULT_COMPACTION_THRESHOLD = 10000;
  private static final long DEFAULT_CACHE_FLUSH_INTERVAL = 0;
  private static final int DEFAULT_CLIENT_RETRIES = 3;

  private HRegion region;
  private RegionScanner scanner;
  private final List<Cell> cells;
  private boolean regionOperationStarted = false;
  private boolean hasMore = true;

  public HBaseRegionScanner(
      Configuration originalConf,
      FileSystem fs,
      Path rootDir,
      TableDescriptor htd,
      RegionInfo hri,
      Scan scan)
      throws IOException {
    Configuration conf = new Configuration(originalConf);
    conf.set("hfile.block.cache.policy", DEFAULT_HFILE_BLOCK_CACHE_POLICY);
    // Set a small default block cache size (32MB). Since we are performing a strictly
    // sequential scan of the snapshot, we read each block once and don't need a large
    // cache for data blocks. 32MB is sufficient to hold HFile index blocks while avoiding
    // OutOfMemory errors on memory-constrained Dataflow workers.
    conf.setIfUnset(
        "hfile.onheap.block.cache.fixed.size", String.valueOf(DEFAULT_HFILE_BLOCK_CACHE_SIZE));
    conf.unset("hbase.bucketcache.ioengine");
    // Setting a huge compaction threshold (10000) effectively disables compactions.
    // Since snapshots are read-only, compactions are useless, and background threads
    // can fail to shut down cleanly during DoFn teardown, causing thread leaks.
    conf.setInt("hbase.hstore.compactionThreshold", DEFAULT_COMPACTION_THRESHOLD);
    // Set flush interval to 0 to disable periodic flushes.
    conf.setLong("hbase.regionserver.optionalcacheflushinterval", DEFAULT_CACHE_FLUSH_INTERVAL);
    conf.setInt("hbase.client.retries.number", DEFAULT_CLIENT_RETRIES);

    scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    htd = TableDescriptorBuilder.newBuilder(htd).setReadOnly(true).build();
    this.region =
        HRegion.newHRegion(
            CommonFSUtils.getTableDir(rootDir, htd.getTableName()),
            (WAL) null,
            fs,
            conf,
            hri,
            htd,
            (RegionServerServices) null);
    this.region.setRestoredRegion(true);

    // Wrap in try-catch to ensure close() is called on failure, avoiding leaks.
    try {
      this.region.initialize();
      this.scanner = this.region.getScanner(scan);
      this.cells = new ArrayList<>();

      this.region.startRegionOperation();
      this.regionOperationStarted = true;
    } catch (Throwable t) {
      LOG.error("Failed to initialize HBaseRegionScanner", t);
      close();
      if (t instanceof IOException) {
        throw (IOException) t;
      }
      throw new IOException("Failed to initialize HBaseRegionScanner", t);
    }
  }

  @Override
  public void close() {
    if (this.scanner != null) {
      try {
        this.scanner.close();
        this.scanner = null;
      } catch (IOException var3) {
        LOG.warn("Exception while closing scanner", var3);
      }
    }

    if (this.region != null) {
      try {
        if (this.regionOperationStarted) {
          try {
            this.region.closeRegionOperation();
          } catch (IOException e) {
            LOG.warn("Exception while closing region operation", e);
          }
        }
        this.region.close(true);
        this.region = null;
      } catch (IOException var2) {
        LOG.warn("Exception while closing region", var2);
      }
    }
  }

  public Result next() throws IOException {
    while (this.hasMore) {
      this.cells.clear();
      this.hasMore = this.scanner.nextRaw(this.cells);

      if (!this.cells.isEmpty()) {
        return Result.create(this.cells);
      }
    }

    return null;
  }
}
