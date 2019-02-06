/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.async;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Tests for {@link AsyncExecutor}
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class TestAsyncExecutor {

  @Mock
  private BigtableDataClient client;

  @SuppressWarnings("rawtypes")
  private SettableFuture future;

  private AsyncExecutor underTest;


  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    underTest = new AsyncExecutor(client);
    future = SettableFuture.create();
  }

  @Test
  public void testNoMutation() {
    Assert.assertFalse(underTest.hasInflightRequests());
  }


  @Test
  public void testReadWriteModify()  {
    when(client.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class))).thenReturn(future);
    underTest.readModifyWriteRowAsync(ReadModifyWriteRowRequest.getDefaultInstance());
    Assert.assertTrue(underTest.hasInflightRequests());
    future.set("");
    Assert.assertFalse(underTest.hasInflightRequests());
  }

  @Test
  public void testInvalidMutation() {
    when(client.readModifyWriteRowAsync(any(ReadModifyWriteRowRequest.class))).thenThrow(new RuntimeException());
    underTest.readModifyWriteRowAsync(ReadModifyWriteRowRequest.getDefaultInstance());
    future.set(ReadRowsResponse.getDefaultInstance());
    Assert.assertFalse(underTest.hasInflightRequests());
  }
}
