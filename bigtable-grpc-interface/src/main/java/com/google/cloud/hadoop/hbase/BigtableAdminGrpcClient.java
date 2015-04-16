package com.google.cloud.hadoop.hbase;

import com.google.bigtable.admin.table.v1.BigtableTableServiceGrpc;
import com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.CreateTableRequest;
import com.google.bigtable.admin.table.v1.DeleteColumnFamilyRequest;
import com.google.bigtable.admin.table.v1.DeleteTableRequest;
import com.google.bigtable.admin.table.v1.ListTablesRequest;
import com.google.bigtable.admin.table.v1.ListTablesResponse;
import com.google.bigtable.admin.table.v1.RenameTableRequest;

import java.util.concurrent.ExecutorService;

/**
 * A gRPC client for accessing the Bigtable Table Admin API.
 */
public class BigtableAdminGrpcClient implements BigtableAdminClient {

  /**
   * Factory method to create an HTTP1 based client.
   * @return A client ready to access bigtable services.
   */
  public static BigtableAdminClient createClient(
      TransportOptions transportOptions,
      ChannelOptions channelOptions,
      ExecutorService executorService) {

    CloseableChannel channel = BigtableChannels.createChannel(
        transportOptions,
        channelOptions,
        executorService);

    return new BigtableAdminGrpcClient(channel);
  }

  protected final CloseableChannel channel;
  private final BigtableTableServiceGrpc.BigtableTableServiceBlockingStub blockingStub;

  /**
   * Create a new BigtableAdminGrpcClient. When constructed, the client takes ownership of the
   * passed CloseableChannel and will invoke close() when the client is close()d.
   */
  public BigtableAdminGrpcClient(
      CloseableChannel closeableChannel) {
    channel = closeableChannel;
    blockingStub = BigtableTableServiceGrpc.newBlockingStub(closeableChannel);
  }

  @Override
  public ListTablesResponse listTables(ListTablesRequest request) {
    return blockingStub.listTables(request);
  }

  @Override
  public void createTable(CreateTableRequest request) {
    blockingStub.createTable(request);
  }

  @Override
  public void createColumnFamily(CreateColumnFamilyRequest request) {
    blockingStub.createColumnFamily(request);
  }

  @Override
  public void deleteTable(DeleteTableRequest request) {
    blockingStub.deleteTable(request);
  }

  @Override
  public void deleteColumnFamily(DeleteColumnFamilyRequest request) {
    blockingStub.deleteColumnFamily(request);
  }

  @Override
  public void renameTable(RenameTableRequest request) {
    blockingStub.renameTable(request);
  }
  
  @Override
  public void close() throws Exception {
    channel.close();
  }
}
