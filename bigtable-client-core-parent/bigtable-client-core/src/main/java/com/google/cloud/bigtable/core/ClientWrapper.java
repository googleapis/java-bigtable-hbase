package com.google.cloud.bigtable.core;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.util.concurrent.ExecutionException;

/**
 * Interface to access Bigtable data service api.
 */
public interface ClientWrapper {

  /**
   * Mutate a row atomically.
   *
   * @param rowMutation a {@link RowMutation} model object.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  void mutateRow(RowMutation rowMutation) throws ExecutionException, InterruptedException;

  /**
   * Mutate a row atomically.
   *
   * @param rowMutation a {@link RowMutation} model object.
   * @return ApiFuture</Void> returns api future.
   * @throws InterruptedException
   */
  ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) throws InterruptedException;

  /**
   * Perform an atomic read-modify-write operation on a row.
   *
   * @param readModifyWriteRow a {@link ReadModifyWriteRow} model object.
   * @return Row a modified row.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow)
      throws ExecutionException, InterruptedException;

  /**
   * Perform an atomic read-modify-write operation on a row.
   *
   * @param readModifyWriteRow a {@link ReadModifyWriteRow} model object.
   * @return ApiFuture<Row> returns future.
   * @throws InterruptedException
   */
  ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) throws InterruptedException;

  /**
   * Creates BulMutation batcher.
   */
  BulkMutation createBulkMutationBatcher();

  /**
   * Mutate a row atomically dependent on a precondition.
   *
   * @param conditionalRowMutation a {@link ConditionalRowMutation} model object.
   * @return ApiFuture<Boolean> returns api future.
   */
  ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation);

  /**
   * Mutate a row atomically dependent on a precondition.
   *
   * @param conditionalRowMutation a {@link ConditionalRowMutation} model object.
   * @return Boolean returns true if predicate returns any result.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation)
      throws ExecutionException, InterruptedException;
}
