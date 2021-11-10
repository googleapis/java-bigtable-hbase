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
package org.apache.hadoop.hbase.regionserver;

import static com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion.batchMutateWithFailures;
import static com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion.processRowThrow;

import com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster.FailingHBaseHRegion;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.wal.WAL;

/**
 * {@link FailingHBaseHRegion} but adapted to HBase 2.x implementation of MiniCluster server.
 *
 * <p>This class has to reside in org.apache.hadoop.hbase.regionserver package because
 * HRegion.RegionScannerImpl returned by {@link FailingHBaseHRegion2#getScanner(Scan, List)} is
 * package-private.
 */
public class FailingHBaseHRegion2 extends HRegion {
  public FailingHBaseHRegion2(
      HRegionFileSystem fs,
      WAL wal,
      Configuration confParam,
      HTableDescriptor htd,
      RegionServerServices rsServices) {
    super(fs, wal, confParam, htd, rsServices);
  }

  public FailingHBaseHRegion2(
      Path tableDir,
      WAL wal,
      FileSystem fs,
      Configuration confParam,
      HRegionInfo regionInfo,
      HTableDescriptor htd,
      RegionServerServices rsServices) {
    super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
  }

  public FailingHBaseHRegion2(
      Path path,
      WAL wal,
      FileSystem fs,
      Configuration conf,
      RegionInfo regionInfo,
      TableDescriptor htd,
      RegionServerServices rsServices) {
    super(path, wal, fs, conf, regionInfo, htd, rsServices);
  }

  @Override
  public HRegion.RegionScannerImpl getScanner(Scan scan, List<KeyValueScanner> additionalScanners)
      throws IOException {
    // HBase 2.x implements Gets as Scans with start row == end row == requested row.
    processRowThrow(scan.getStartRow());
    return super.getScanner(scan, additionalScanners);
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    processRowThrow(rm.getRow());
    super.mutateRow(rm);
  }

  @Override
  public OperationStatus[] batchMutate(
      Mutation[] mutations, boolean atomic, long nonceGroup, long nonce) throws IOException {
    return batchMutateWithFailures(
        mutations, (m) -> super.batchMutate(m, atomic, nonceGroup, nonce));
  }

  @Override
  public OperationStatus[] batchMutate(Mutation[] mutations, long nonceGroup, long nonce)
      throws IOException {
    return batchMutateWithFailures(mutations, (m) -> super.batchMutate(m, nonceGroup, nonce));
  }

  @Override
  public Result get(Get get) throws IOException {
    processRowThrow(get.getRow());
    return super.get(get);
  }

  @Override
  public Result increment(Increment mutation, long nonceGroup, long nonce) throws IOException {
    processRowThrow(mutation.getRow());
    return super.increment(mutation, nonceGroup, nonce);
  }

  @Override
  public Result append(Append mutation, long nonceGroup, long nonce) throws IOException {
    processRowThrow(mutation.getRow());
    return super.append(mutation, nonceGroup, nonce);
  }
}
