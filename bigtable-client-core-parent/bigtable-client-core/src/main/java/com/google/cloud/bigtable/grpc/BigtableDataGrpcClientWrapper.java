package com.google.cloud.bigtable.grpc;

import com.google.api.core.ApiFuture;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

public class BigtableDataGrpcClientWrapper implements IBigtableDataClient {

  private BigtableDataGrpcClient bigtableDataGrpcClient;
  private final RequestContext requestContext;

  public BigtableDataGrpcClientWrapper(BigtableDataGrpcClient bigtableDataGrpcClient,
      BigtableOptions options) {
    this.bigtableDataGrpcClient = bigtableDataGrpcClient;
    this.requestContext = RequestContext
        .create(InstanceName.of(options.getProjectId(),
            options.getInstanceId()),
            options.getAppProfileId()
        );
  }

  @Override
  public void mutateRow(RowMutation rowMutation) {
    MutateRowRequest mutateRowRequest = rowMutation.toProto(requestContext);
    bigtableDataGrpcClient.mutateRow(mutateRowRequest);
  }

  @Override
  public ApiFuture<Void> mutateRowAsync(RowMutation rowMutation) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Row readModifyWriteRow(ReadModifyWriteRow readModifyWriteRow) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ApiFuture<Row> readModifyWriteRowAsync(ReadModifyWriteRow readModifyWriteRow) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public IBulkMutation createBulkMutationBatcher() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public ApiFuture<Boolean> checkAndMutateRowAsync(ConditionalRowMutation conditionalRowMutation) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Boolean checkAndMutateRow(ConditionalRowMutation conditionalRowMutation) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
