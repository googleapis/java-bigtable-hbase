/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.InternalException;
import com.google.bigtable.v2.BigtableGrpc;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestRetryRstStream {

  private BigtableSession session;
  private ServerBuilder serverBuilder = ServerBuilder.forPort(1234);
  private Server server;
  private AtomicInteger attemptCount = new AtomicInteger(0);

  @Before
  public void setup() throws IOException {
    server = serverBuilder.addService(new FakeBigtableService()).build();

    server.start();

    session =
        new BigtableSession(
            BigtableOptions.builder()
                .setProjectId("fake-project")
                .setInstanceId("fake-instance")
                .setUserAgent("test")
                .enableEmulator("localhost:1234")
                .build());
  }

  @Test
  public void testReadRowsIsRetried() throws IOException {
    try {
      ResultScanner<Row> rows =
          session.getDataClient().readRows(ReadRowsRequest.getDefaultInstance());
      rows.next();
    } catch (Exception e) {
      Assert.fail("rst errors should be retried");
    }
    Assert.assertEquals(2, attemptCount.get());
  }

  @Test
  public void testMutateRowsIsRetried() {
    try {
      List<MutateRowsResponse> responses =
          session.getDataClient().mutateRows(MutateRowsRequest.getDefaultInstance());
      responses.get(0);
    } catch (Exception e) {
      Assert.fail("rst errors should be retried");
    }
    Assert.assertEquals(2, attemptCount.get());
  }

  @Test
  public void testCheckAndMutateIsNotRetried() {
    try {
      session.getDataClient().checkAndMutateRow(CheckAndMutateRowRequest.getDefaultInstance());
      Assert.fail("rst errors should not be retried for check and mutate");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Received Rst Stream"));
    }
    Assert.assertEquals(1, attemptCount.get());
  }

  @After
  public void close() {
    server.shutdown();
  }

  private class FakeBigtableService extends BigtableGrpc.BigtableImplBase {
    @Override
    public void readRows(
        ReadRowsRequest request, StreamObserver<ReadRowsResponse> responseObserver) {
      if (attemptCount.getAndIncrement() == 0) {
        responseObserver.onError(
            new InternalException(
                new StatusRuntimeException(
                    Status.INTERNAL.withDescription(
                        "INTERNAL: HTTP/2 error code: INTERNAL_ERROR\nReceived Rst Stream")),
                GrpcStatusCode.of(Status.Code.INTERNAL),
                false));
      } else {
        responseObserver.onNext(ReadRowsResponse.getDefaultInstance());
        responseObserver.onCompleted();
      }
    }

    @Override
    public void mutateRows(
        MutateRowsRequest request, StreamObserver<MutateRowsResponse> responseObserver) {
      if (attemptCount.getAndIncrement() == 0) {
        responseObserver.onError(
            new InternalException(
                new StatusRuntimeException(
                    Status.INTERNAL.withDescription(
                        "INTERNAL: HTTP/2 error code: INTERNAL_ERROR\nReceived Rst Stream")),
                GrpcStatusCode.of(Status.Code.INTERNAL),
                false));
      } else {
        responseObserver.onNext(MutateRowsResponse.getDefaultInstance());
        responseObserver.onCompleted();
      }
    }

    @Override
    public void checkAndMutateRow(
        CheckAndMutateRowRequest request,
        StreamObserver<CheckAndMutateRowResponse> responseObserver) {
      if (attemptCount.getAndIncrement() == 0) {
        responseObserver.onError(
            new InternalException(
                new StatusRuntimeException(
                    Status.INTERNAL.withDescription(
                        "INTERNAL: HTTP/2 error code: INTERNAL_ERROR\nReceived Rst Stream")),
                GrpcStatusCode.of(Status.Code.INTERNAL),
                false));
      } else {
        responseObserver.onNext(CheckAndMutateRowResponse.getDefaultInstance());
        responseObserver.onCompleted();
      }
    }
  }
}
