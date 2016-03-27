/*
 * Copyright 2015 Google Inc. All Rights Reserved. Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.google.cloud.bigtable.grpc.async;

import static org.mockito.Mockito.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.MutateRowsRequest;
import com.google.bigtable.v1.MutateRowsRequest.Entry;
import com.google.bigtable.v1.MutateRowsResponse;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.Mutation.SetCell;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.rpc.Status;
import com.google.rpc.Status.Builder;

import io.grpc.StatusRuntimeException;

/**
 * Tests for {@link BulkMutation}
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@RunWith(JUnit4.class)
public class TestBulkMutation {
  private final static String TABLE_NAME = "table";
  private final static ByteString QUALIFIER = ByteString.copyFrom("qual".getBytes());

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  private AsyncExecutor asyncExecutor;

  @Mock
  private RpcThrottler rpcThrottler;

  @Mock
  private ListenableFuture mockFuture;

  BulkMutation.Batch underTest;

  @Before
  public void setup() throws InterruptedException {
    MockitoAnnotations.initMocks(this);
    when(asyncExecutor.getRpcThrottler()).thenReturn(rpcThrottler);
    when(asyncExecutor.mutateRowsAsync(any(MutateRowsRequest.class))).thenReturn(mockFuture);
    when(asyncExecutor.addMutationRetry(any(ListenableFuture.class), any(MutateRowRequest.class)))
        .thenAnswer(new Answer<ListenableFuture>() {
          @Override
          public ListenableFuture answer(InvocationOnMock invocation) throws Throwable {
            return invocation.getArgumentAt(0, ListenableFuture.class);
          }
        });
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(0, Runnable.class).run();
        return null;
      }
    }).when(mockFuture).addListener(any(Runnable.class), any(Executor.class));
    underTest = new BulkMutation.Batch(TABLE_NAME, asyncExecutor, 10000, 10000);
  }

  @Test
  public void testAdd() {
    MutateRowRequest mutateRowRequest = createRequest();
    underTest.add(mutateRowRequest);
    MutateRowsRequest expected = MutateRowsRequest.newBuilder()
        .setTableName(TABLE_NAME)
        .addEntries(Entry.newBuilder().addMutations(mutateRowRequest.getMutations(0)).build())
        .build();
    Assert.assertEquals(expected, underTest.toRequest());
  }

  protected MutateRowRequest createRequest() {
    return MutateRowRequest.newBuilder().addMutations(
      Mutation.newBuilder()
        .setSetCell(
            SetCell.newBuilder()
              .setFamilyName("cf1")
              .setColumnQualifier(QUALIFIER))
            .build())
        .build();
  }

  @Test
  public void testCallableSuccess() throws InterruptedException, ExecutionException {
    ListenableFuture<Empty> rowFuture = underTest.add(createRequest());
    setResponse(io.grpc.Status.OK);
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(Empty.getDefaultInstance(), rowFuture.get());
  }

  @Test
  public void testCallableNotOKStatus() throws InterruptedException, ExecutionException {
    ListenableFuture<Empty> rowFuture = underTest.add(createRequest());
    SettableFuture<MutateRowsResponse> rowsFuture = SettableFuture.<MutateRowsResponse> create();
    underTest.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture.isDone());
    setResponse(io.grpc.Status.NOT_FOUND);

    try {
      rowFuture.get();
     } catch (ExecutionException e) {
      Assert.assertEquals(StatusRuntimeException.class, e.getCause().getClass());
    }
  }

  @Test
  public void testCallableTooFewStatuses() throws InterruptedException, ExecutionException {
    ListenableFuture<Empty> rowFuture1 = underTest.add(createRequest());
    ListenableFuture<Empty> rowFuture2 = underTest.add(createRequest());
    SettableFuture<MutateRowsResponse> rowsFuture = SettableFuture.<MutateRowsResponse> create();
    underTest.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());
    setResponse(io.grpc.Status.OK);

    Assert.assertTrue(rowFuture1.isDone());
    Assert.assertTrue(rowFuture2.isDone());
    Assert.assertEquals(Empty.getDefaultInstance(), rowFuture1.get());
    try {
      rowFuture2.get();
     } catch (ExecutionException e) {
      Assert.assertEquals(StatusRuntimeException.class, e.getCause().getClass());
      Assert.assertEquals(io.grpc.Status.Code.UNKNOWN,
        ((StatusRuntimeException) e.getCause()).getStatus().getCode());
    }
  }

  @Test
  public void testCallableException() throws InterruptedException {
    ListenableFuture<Empty> rowFuture1 = underTest.add(createRequest());
    ListenableFuture<Empty> rowFuture2 = underTest.add(createRequest());
    SettableFuture<MutateRowsResponse> rowsFuture = SettableFuture.<MutateRowsResponse> create();
    underTest.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());
    RuntimeException throwable = new RuntimeException();
    rowsFuture.setException(throwable);
    Assert.assertTrue(rowFuture1.isDone());
    Assert.assertTrue(rowFuture2.isDone());

    try {
      rowFuture1.get();
      Assert.fail("expected an exception");
    } catch (ExecutionException e) {
      Assert.assertEquals(throwable, e.getCause());
    }
  }

  private void setResponse(final io.grpc.Status code)
      throws InterruptedException, ExecutionException {
    Builder notFound = Status.newBuilder().setCode(code.getCode().value());
    MutateRowsResponse response =
        MutateRowsResponse.newBuilder().addStatuses(notFound).build();
    when(mockFuture.get()).thenReturn(response);
    underTest.sendRows();
  }
}
