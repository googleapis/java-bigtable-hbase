package com.google.cloud.bigtable.grpc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ResumingBigtableResultScanner;
import com.google.cloud.bigtable.grpc.scanner.RetryingStreamingResultScanner;
import com.google.cloud.bigtable.grpc.scanner.StreamingBigtableResultScanner;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.grpc.Call;
import io.grpc.Channel;

import java.util.concurrent.ExecutorService;

/**
 * Test for the {@link BigtableDataGrpcClient}.
 */
@RunWith(JUnit4.class)
public class BigtableDataGrpcClientTest {

  @Mock
  Channel mockChannel;
  @Mock
  ExecutorService mockExecutorService;
  @Mock
  Call<ReadRowsRequest, ReadRowsResponse> mockCall;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(mockChannel.newCall(BigtableServiceGrpc.CONFIG.readRows)).thenReturn(mockCall);
  }

  @Test
  public void testReadRows_retriable() {
    RetryOptions retryOptions =
        new RetryOptions.Builder()
            .setEnableRetries(true)
            .setRetryOnDeadlineExceeded(true)
            .setInitialBackoffMillis(100)
            .setBackoffMultiplier(2D)
            .setMaxElapsedBackoffMillis(500)
            .build();
    BigtableDataGrpcClient client =
        new BigtableDataGrpcClient(mockChannel, mockExecutorService, retryOptions);
    ResultScanner<Row> scanner = client.readRows(ReadRowsRequest.getDefaultInstance());
    assertTrue(scanner instanceof  RetryingStreamingResultScanner);
  }

  @Test
  public void testReadRows_retriable_resumable() {
    RetryOptions retryOptions =
        new RetryOptions.Builder()
            .setEnableRetries(true)
            .setRetryOnDeadlineExceeded(true)
            .setInitialBackoffMillis(100)
            .setBackoffMultiplier(2D)
            .setMaxElapsedBackoffMillis(500)
            .build();
    BigtableDataGrpcClient client =
        new BigtableDataGrpcClient(mockChannel, mockExecutorService, retryOptions);
    ResultScanner<Row> scanner = client.readRows(ReadRowsRequest.getDefaultInstance(), true);
    assertTrue(scanner instanceof  RetryingStreamingResultScanner);
  }

  @Test
  public void testReadRows_resumable() {
    RetryOptions retryOptions =
        new RetryOptions.Builder()
            .setEnableRetries(false)
            .setRetryOnDeadlineExceeded(true)
            .setInitialBackoffMillis(100)
            .setBackoffMultiplier(2D)
            .setMaxElapsedBackoffMillis(500)
            .build();
    BigtableDataGrpcClient client =
        new BigtableDataGrpcClient(mockChannel, mockExecutorService, retryOptions);
    ReadRowsRequest requestWithWileMatchFilter = ReadRowsRequest.newBuilder()
        .setFilter(RowFilter.newBuilder().setApplyLabelTransformer("label"))
        .build();
    ResultScanner<Row> scanner = client.readRows(requestWithWileMatchFilter, true);
    assertTrue(scanner instanceof  ResumingBigtableResultScanner);
  }

  @Test
  public void testReadRows_nonresumable() {
    RetryOptions retryOptions =
        new RetryOptions.Builder()
            .setEnableRetries(false)
            .setRetryOnDeadlineExceeded(true)
            .setInitialBackoffMillis(100)
            .setBackoffMultiplier(2D)
            .setMaxElapsedBackoffMillis(500)
            .build();
    BigtableDataGrpcClient client =
        new BigtableDataGrpcClient(mockChannel, mockExecutorService, retryOptions);
    ResultScanner<Row> scanner = client.readRows(ReadRowsRequest.getDefaultInstance());
    assertTrue(scanner instanceof  StreamingBigtableResultScanner);
  }
}

