package com.google.cloud.bigtable.grpc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutorService;

@RunWith(JUnit4.class)
public class BigtableGrpcClientTest {

  @Mock
  private ExecutorService executorService;
  
  private TransportOptions transportOptions;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);

    transportOptions = new TransportOptions(TransportOptions.BigtableTransports.HTTP2_NETTY_TLS,
        "google.com", 123);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void RetryOnDeadlineExceeded_enabled() {
    ChannelOptions channelOptions = new ChannelOptions.Builder()
        .setRetryOptions(new RetryOptions.Builder()
            .setEnableRetries(true)
            .setBackoffMultiplier(20)
            .setRetryOnDeadlineExceeded(true)
            .build())
        .setUserAgent("test user agent")
        .build();
    BigtableGrpcClient bigtableGrpcClient = (BigtableGrpcClient) BigtableGrpcClient.createClient(
        transportOptions, channelOptions, executorService);
    RetryOptions streamingRetryOptions =
        bigtableGrpcClient.getClientOptions().getStreamingRetryOptions();
    assertTrue(streamingRetryOptions.retryOnDeadlineExceeded());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void RetryOnDeadlineExceeded_disabled() {
    ChannelOptions channelOptions = new ChannelOptions.Builder()
        .setRetryOptions(new RetryOptions.Builder()
          .setEnableRetries(true)
          .setBackoffMultiplier(20)
          .setRetryOnDeadlineExceeded(false)
          .build())
        .setUserAgent("test user agent")
        .build();
    BigtableGrpcClient bigtableGrpcClient = (BigtableGrpcClient) BigtableGrpcClient.createClient(
        transportOptions, channelOptions, executorService);
    RetryOptions streamingRetryOptions =
        bigtableGrpcClient.getClientOptions().getStreamingRetryOptions();
    assertFalse(streamingRetryOptions.retryOnDeadlineExceeded());
  }
}
