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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.wal.WAL;

/**
 * Implementation of {@link HRegion} that rejects operations on registered rows. Used to simulate
 * server-side errors in integration tests which use in-memory MiniCluster as HBase server.
 */
public class FailingHBaseHRegion extends HRegion {
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
  public void mutateRow(RowMutations rm) throws IOException {
    processRowThrow(rm.getRow());
    super.mutateRow(rm);
  }

  @Override
  public OperationStatus[] batchMutate(
      Mutation[] mutations, final long nonceGroup, final long nonce) throws IOException {
    return batchMutateWithFailures(
        mutations,
        new FunctionThrowing<Mutation[], OperationStatus[], IOException>() {
          @Override
          public OperationStatus[] apply(Mutation[] mutations) throws IOException {
            return FailingHBaseHRegion.super.batchMutate(mutations, nonceGroup, nonce);
          }
        });
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

  private static Map<ByteBuffer, OperationStatus> fakeErrorsMap = new ConcurrentHashMap<>();
  private static Map<Predicate<byte[]>, OperationStatus> errorConditionMap =
      new ConcurrentHashMap<>();

  public static void failMutation(byte[] row, String message) {
    failMutation(row, OperationStatusCode.FAILURE, message);
  }

  public static void failMutation(byte[] row, OperationStatusCode statusCode, String message) {
    ByteBuffer byteBufferRow = ByteBuffer.wrap(row);
    Preconditions.checkArgument(!fakeErrorsMap.containsKey(byteBufferRow));
    fakeErrorsMap.put(byteBufferRow, new OperationStatus(statusCode, message));
  }

  public static void failMutation(Predicate<byte[]> failCondition, String message) {
    failMutation(failCondition, OperationStatusCode.FAILURE, message);
  }

  public static void failMutation(
      Predicate<byte[]> failCondition, OperationStatusCode statusCode, String message) {
    errorConditionMap.put(failCondition, new OperationStatus(statusCode, message));
  }

  public static void clearFailures() {
    fakeErrorsMap.clear();
    errorConditionMap.clear();
  }

  public static OperationStatus processRowNoThrow(byte[] rowKey) {
    ByteBuffer row = ByteBuffer.wrap(rowKey);
    if (fakeErrorsMap.containsKey(row)) {
      return fakeErrorsMap.get(row);
    }
    for (Entry<Predicate<byte[]>, OperationStatus> entry : errorConditionMap.entrySet()) {
      if (entry.getKey().apply(rowKey)) {
        return entry.getValue();
      }
    }
    return null;
  }

  public static void processRowThrow(byte[] rowKey) throws IOException {
    throwError(processRowNoThrow(rowKey));
  }

  private static void throwError(OperationStatus operationStatus) throws IOException {
    if (operationStatus == null) {
      return;
    }

    if (operationStatus.getOperationStatusCode().equals(OperationStatusCode.SANITY_CHECK_FAILURE)) {
      throw new FailedSanityCheckException(operationStatus.getExceptionMsg());
    } else if (operationStatus.getOperationStatusCode().equals(OperationStatusCode.BAD_FAMILY)) {
      throw new NoSuchColumnFamilyException(operationStatus.getExceptionMsg());
    } else if (operationStatus.getOperationStatusCode().equals(OperationStatusCode.FAILURE)) {
      throw new DoNotRetryIOException(operationStatus.getExceptionMsg());
    } else {
      throw new DoNotRetryIOException(operationStatus.getExceptionMsg());
    }
  }

  public interface FunctionThrowing<T, R, E extends Throwable> {
    R apply(T t) throws E;
  }

  public static OperationStatus[] batchMutateWithFailures(
      Mutation[] mutations, FunctionThrowing<Mutation[], OperationStatus[], IOException> op)
      throws IOException {
    OperationStatus[] result = new OperationStatus[mutations.length];
    List<Mutation> mutationsToRun = new ArrayList<>();

    // We fill some positions in result[] if the mutation is to err
    // according to fakeErrorsMap and errorConditionMap.
    for (int i = 0; i < mutations.length; i++) {
      Mutation mutation = mutations[i];
      OperationStatus r = processRowNoThrow(mutation.getRow());
      if (r != null) {
        result[i] = r;
      } else {
        mutationsToRun.add(mutation);
      }
    }

    // We fill the remaining positions in result[] with results of op().
    OperationStatus[] superResult = op.apply(mutationsToRun.toArray(new Mutation[0]));
    int correspondingSuperResultIdx = 0;
    for (int i = 0; i < mutations.length; i++) {
      if (result[i] == null) {
        result[i] = superResult[correspondingSuperResultIdx];
        correspondingSuperResultIdx++;
      }
    }
    return result;
  }
}
