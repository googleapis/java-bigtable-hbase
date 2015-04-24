package com.google.cloud.bigtable.grpc;

import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.CheckAndMutateRowResponse;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.ServiceException;

import java.util.List;

/**
 * Interface to access v1 final Bigtable service methods
 */
public interface BigtableClient extends AutoCloseable {

  /**
   * Mutate a row atomically.
   */
  Empty mutateRow(MutateRowRequest request) throws ServiceException;

  /**
   * Mutate a row atomically, returning a Future that will finish when
   * the mutation has completed.
   */
  ListenableFuture<Empty> mutateRowAsync(MutateRowRequest request);

  /**
   * Mutate a row atomically dependant on a precondition.
   */
  CheckAndMutateRowResponse checkAndMutateRow(CheckAndMutateRowRequest request)
      throws ServiceException;

  /**
   * Mutate a row atomically dependant on a precondition, returning a Future that will finish when
   * the mutation has completed.
   */
  ListenableFuture<CheckAndMutateRowResponse> checkAndMutateRowAsync(
      CheckAndMutateRowRequest request);

  /**
   * Perform an atomic read-modify-write operation on a row.
   */
  Row readModifyWriteRow(ReadModifyWriteRowRequest request);

  /**
   * Perform an atomic read-modify-write operation on a row, returning a Future that will complete
   * when the mutation has completed.
   */
  ListenableFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRowRequest request);

  /**
   * Sample row keys from a table.
   */
  ImmutableList<SampleRowKeysResponse> sampleRowKeys(SampleRowKeysRequest request);

  /**
   * Sample row keys from a table, returning a Future that will complete when the sampling has
   * completed.
   */
  ListenableFuture<ImmutableList<SampleRowKeysResponse>> sampleRowKeysAsync(
      SampleRowKeysRequest request);

  /**
   * Perform a scan over rows.
   */
  ResultScanner<Row> readRows(ReadRowsRequest request);

  /**
   * Read multiple Rows into an in-memory list, returning a Future that will complete when the
   * readRows call has completed.
   */
  ListenableFuture<List<Row>> readRowsAsync(ReadRowsRequest request);
}
