/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.hbase.mirroring.utils.failinghbaseminicluster;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.wal.WAL;

public class FailingHBaseHRegion extends HRegion {
  private static Map<ByteBuffer, String> fakeErrorsMap = new ConcurrentHashMap<>();

  public static void failMutation(byte[] row, String message) {
    ByteBuffer byteBufferRow = ByteBuffer.wrap(row);
    assert !fakeErrorsMap.containsKey(byteBufferRow);
    fakeErrorsMap.put(byteBufferRow, message);
  }

  public static void clearFailures() {
    fakeErrorsMap.clear();
  }

  public FailingHBaseHRegion(
      HRegionFileSystem fs,
      WAL wal,
      Configuration confParam,
      HTableDescriptor htd,
      RegionServerServices rsServices) {
    super(fs, wal, confParam, htd, rsServices);
  }

  public FailingHBaseHRegion(
      Path tableDir,
      WAL wal,
      FileSystem fs,
      Configuration confParam,
      HRegionInfo regionInfo,
      HTableDescriptor htd,
      RegionServerServices rsServices) {
    super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
  }

  @Override
  public OperationStatus[] batchMutate(Mutation[] mutations, long nonceGroup, long nonce)
      throws IOException {

    OperationStatus[] result = new OperationStatus[mutations.length];
    List<Mutation> mutationsToRun = new ArrayList<>();

    for (int i = 0; i < mutations.length; i++) {
      Mutation mutation = mutations[i];
      OperationStatus r = processRow(mutation);
      if (r != null) {
        result[i] = r;
      } else {
        mutationsToRun.add(mutation);
      }
    }
    OperationStatus[] superResult =
        super.batchMutate(mutationsToRun.toArray(new Mutation[0]), nonceGroup, nonce);
    int redIndex = 0;
    for (int i = 0; i < mutations.length; i++) {
      if (result[i] == null) {
        result[i] = superResult[redIndex];
        redIndex++;
      }
    }
    return result;
  }

  private static OperationStatus processRow(Mutation mutation) {
    ByteBuffer row = ByteBuffer.wrap(mutation.getRow());
    if (fakeErrorsMap.containsKey(row)) {
      return new OperationStatus(OperationStatusCode.FAILURE, fakeErrorsMap.get(row));
    }
    return null;
  }
}
