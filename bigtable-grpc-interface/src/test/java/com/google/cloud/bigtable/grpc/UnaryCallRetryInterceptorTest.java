/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

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
