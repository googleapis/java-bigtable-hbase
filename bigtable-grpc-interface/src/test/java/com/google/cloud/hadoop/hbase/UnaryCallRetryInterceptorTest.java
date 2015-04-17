package com.google.cloud.hadoop.hbase;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.bigtable.v1.Row;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Empty;

import io.grpc.Call;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@RunWith(JUnit4.class)
public class UnaryCallRetryInterceptorTest {

  // Setting this high has helped find race conditions:
  private static final int DEFAULT_RETRY_MILLIS = 100;
  private static final double DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2;
  private static final int DEFAULT_RETRY_MAX_ELAPSED_TIME_MILLIS = 60 * 1000; // 60 seconds

  @Mock
  private Channel channelStub;
  @Mock
  private Call<MutateRowRequest, Empty> callStub;

  private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);

  private final ImmutableSet<MethodDescriptor<?, ?>> retriableMethods =
      new ImmutableSet.Builder<MethodDescriptor<?, ?>>()
          .add(BigtableServiceGrpc.CONFIG.mutateRow)
          .build();

  private UnaryCallRetryInterceptor retryInterceptor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    retryInterceptor =
        new UnaryCallRetryInterceptor(
            channelStub,
            executorService,
            retriableMethods,
            DEFAULT_RETRY_MILLIS,
            DEFAULT_RETRY_BACKOFF_MULTIPLIER,
            DEFAULT_RETRY_MAX_ELAPSED_TIME_MILLIS);

    when(channelStub.newCall(eq(BigtableServiceGrpc.CONFIG.mutateRow))).thenReturn(callStub);
  }

  @Test
  public void retriableMethodsAreWrappedInRetryingCall() {
    Call<MutateRowRequest, Empty> mutateRowCall =
        retryInterceptor.newCall(BigtableServiceGrpc.CONFIG.mutateRow);

    Assert.assertTrue("mutateRowCall should be a RetryingCall",
        mutateRowCall instanceof RetryingCall);
  }

  @Test
  public void nonRetriableMethodsAreNotWrappedInRetryingCall() {
    Call<ReadModifyWriteRowRequest, Row> readModifyWriteRowCall =
        retryInterceptor.newCall(BigtableServiceGrpc.CONFIG.readModifyWriteRow);

    Assert.assertFalse("readModifyWriteRowCall should not be a RetryingCall",
        readModifyWriteRowCall instanceof RetryingCall);
  }
}
