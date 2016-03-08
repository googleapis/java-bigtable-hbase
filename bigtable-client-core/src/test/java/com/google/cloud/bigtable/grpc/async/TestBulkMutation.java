/*
 * Copyright 2015 Google Inc. All Rights Reserved. Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.google.cloud.bigtable.grpc.async;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.MutateRowsRequest;
import com.google.bigtable.v1.MutateRowsRequest.Entry;
import com.google.bigtable.v1.MutateRowsResponse;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.Mutation.SetCell;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.rpc.Status;
import com.google.rpc.Status.Builder;

import io.grpc.StatusRuntimeException;

/**
 * Tests for {@link BulkMutation}
 */
@RunWith(JUnit4.class)
public class TestBulkMutation {
  final static String tableName = "table";
  final static ByteString qualifier = ByteString.copyFrom("qual".getBytes());

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testAdd() {
    BulkMutation underTest = new BulkMutation(tableName);
    MutateRowRequest mutateRowRequest = createRequest();
    underTest.add(mutateRowRequest);
    MutateRowsRequest expected = MutateRowsRequest.newBuilder()
        .setTableName(tableName)
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
              .setColumnQualifier(qualifier))
            .build())
        .build();
  }

  @Test
  public void testCallableSuccess() throws InterruptedException, ExecutionException {
    BulkMutation underTest = new BulkMutation(tableName);
    SettableFuture<Empty> rowFuture = underTest.add(createRequest());
    SettableFuture<MutateRowsResponse> rowsFuture = SettableFuture.<MutateRowsResponse> create();
    underTest.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture.isDone());
    Builder okStatus = Status.newBuilder().setCode(io.grpc.Status.OK.getCode().value());
    rowsFuture.set(MutateRowsResponse.newBuilder().addStatuses(okStatus).build());
    Assert.assertTrue(rowFuture.isDone());
    Assert.assertEquals(Empty.getDefaultInstance(), rowFuture.get());
  }

  @Test
  public void testCallableNotOKStatus() throws InterruptedException {
    BulkMutation underTest = new BulkMutation(tableName);
    SettableFuture<Empty> rowFuture = underTest.add(createRequest());
    SettableFuture<MutateRowsResponse> rowsFuture = SettableFuture.<MutateRowsResponse> create();
    underTest.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture.isDone());
    Builder okStatus = Status.newBuilder().setCode(io.grpc.Status.NOT_FOUND.getCode().value());
    rowsFuture.set(MutateRowsResponse.newBuilder().addStatuses(okStatus).build());

    try {
      rowFuture.get();
     } catch (ExecutionException e) {
      Assert.assertEquals(StatusRuntimeException.class, e.getCause().getClass());
    }
  }

  @Test
  public void testCallableTooFewStatuses() throws InterruptedException, ExecutionException {
    BulkMutation underTest = new BulkMutation(tableName);
    SettableFuture<Empty> rowFuture1 = underTest.add(createRequest());
    SettableFuture<Empty> rowFuture2 = underTest.add(createRequest());
    SettableFuture<MutateRowsResponse> rowsFuture = SettableFuture.<MutateRowsResponse> create();
    underTest.addCallback(rowsFuture);
    Assert.assertFalse(rowFuture1.isDone());
    Assert.assertFalse(rowFuture2.isDone());
    Builder okStatus = Status.newBuilder().setCode(io.grpc.Status.OK.getCode().value());
    rowsFuture.set(MutateRowsResponse.newBuilder().addStatuses(okStatus).build());

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
    BulkMutation underTest = new BulkMutation(tableName);
    SettableFuture<Empty> rowFuture1 = underTest.add(createRequest());
    SettableFuture<Empty> rowFuture2 = underTest.add(createRequest());
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
}
