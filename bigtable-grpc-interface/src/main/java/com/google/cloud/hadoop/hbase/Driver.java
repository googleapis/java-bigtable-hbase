package com.google.cloud.hadoop.hbase;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.flags.Flag;
import com.google.common.flags.FlagSpec;
import com.google.common.flags.Flags;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Test CLI for bigtable clients
 */
public class Driver {

  /**
   * The operation to perform
   */
  public enum Operation {
    /**
     * Performs a mutateRow bigtable call.
     */
    MUTATE_ROW {
      private ByteString getWriteValue() {
        String stringToWrite = writeValue.get();
        return ByteString.copyFromUtf8(stringToWrite);
      }

      @Override
      public GeneratedMessage execute(BigtableClient btClient) throws ServiceException {
        MutateRowRequest.Builder request = MutateRowRequest.newBuilder()
            .setTableName("projects/" + projectId.get() 
                          + "zones/zone/clusters/cluster/tables/" + tableName.get())
            .setRowKey(ByteString.copyFromUtf8(rowKey.get()));

        Mutation.Builder mutBuilder = request.addMutationsBuilder();
        mutBuilder.getSetCellBuilder()
            .setFamilyName(familyName.get())
            .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier.get()))
            .setTimestampMicros(timestampMicros.get())
            .setValue(getWriteValue());

        Empty response = btClient.mutateRow(request.build());

        return response;
      }
    },
    /**
     * Performs a getRow bigtable call.
     */
    GET_ROW {
      @Override
      public GeneratedMessage execute(BigtableClient btClient) 
          throws ServiceException, IOException {
        byte[] rowKeyBytes = StandardCharsets.UTF_8.encode(rowKey.get()).array();
        ReadRowsRequest.Builder request = ReadRowsRequest.newBuilder()
            .setTableName("projects/" + projectId.get()
                + "/zones/zone/clusters/cluster/tables" + tableName.get())
            .setRowKey(ByteString.copyFrom(rowKeyBytes));
        ResultScanner<Row> scanner = btClient.readRows(request.build());
        Row ret = scanner.next();
        scanner.close();
        return (ret == null 
            ? Row.newBuilder().setKey(ByteString.copyFrom(rowKeyBytes)).build()
            : ret);
      }
    };

    public abstract GeneratedMessage execute(BigtableClient btClient) 
        throws ServiceException, IOException;
  }

  @FlagSpec(
      help = "The host:port pair to use to connect to apiary.",
      altName = "host_port")
  public static Flag<String> hostPort = Flag.value("bigtable.googleapis.com:443");

  @FlagSpec(
      help = "False to use the asynchronous RPC interface",
      altName = "use_blocking_interface")
  public static Flag<Boolean> useBlockingInterface = Flag.value(true);

  @FlagSpec(
      help = "The cloud project id to identify the tables being used.",
      altName = "project_id")
  public static Flag<String> projectId = Flag.value("");

  @FlagSpec(
      help = "The table to operate on",
      altName = "table_name")
  public static Flag<String> tableName = Flag.value("");

  @FlagSpec(
      help = "Rowkey to operate on",
      altName = "row_key")
  public static Flag<String> rowKey = Flag.value("");

  @FlagSpec(
      help = "Family to operate on",
      altName = "family_name")
  public static Flag<String> familyName = Flag.value("");

  @FlagSpec(
      help = "Column to operate on within familyName",
      altName = "column_qualifier")
  public static Flag<String> columnQualifier = Flag.value("");

  @FlagSpec(
      help = "Timestamp to write (in micros)",
      altName = "timestamp_micros")
  public static Flag<Long> timestampMicros = Flag.value(0L);

  @FlagSpec(
      help = "A string whose UTF-8 byte representation will be written to bigtable",
      altName = "write_value")
  public static Flag<String> writeValue = Flag.value("");

  @FlagSpec(help = "The operation to perform")
  public static Flag<Operation> operation = Flag.value(Operation.GET_ROW);

  public static void main (String[] args) throws ServiceException, IOException {
    Flags.parse(args);
    Driver driver = new Driver();
    driver.run();
  }

  public void run() throws ServiceException, IOException {
    String hostPortString = hostPort.get();
    List<String> splitHostPort = Splitter.on(':').trimResults().splitToList(hostPortString);
    Preconditions.checkArgument(splitHostPort.size() == 2,
        String.format("hostPort should be of the form host:port, but found: '%s'", hostPortString));
    String host = splitHostPort.get(0);
    String portString = splitHostPort.get(1);
    int port;
    try {
      port = Integer.parseInt(portString);
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Port part of hostPort must be an integer.", nfe);
    }

    TransportOptions transportOptions = new TransportOptions(
        TransportOptions.BigtableTransports.HTTP2_NETTY_TLS,
        host,
        port);

    ExecutorService executorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("bigtable-nio-%s")
            .build());

    ChannelOptions channelOptions = new ChannelOptions.Builder().build();

    BigtableClient btClient = BigtableGrpcClient.createClient(
        transportOptions, channelOptions, executorService);

    GeneratedMessage result = operation.get().execute(btClient);
    TextFormat.print(result, System.out);
  }
}
