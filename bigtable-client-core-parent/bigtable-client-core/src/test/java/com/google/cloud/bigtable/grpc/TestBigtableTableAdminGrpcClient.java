/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.api.client.testing.util.MockBackOff;
import com.google.bigtable.admin.v2.BigtableTableAdminGrpc;
import com.google.bigtable.admin.v2.CheckConsistencyRequest;
import com.google.bigtable.admin.v2.CheckConsistencyResponse;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenRequest;
import com.google.bigtable.admin.v2.GenerateConsistencyTokenResponse;
import com.google.cloud.bigtable.config.BigtableOptions;

import io.grpc.Status;
import io.grpc.testing.GrpcServerRule;
import io.grpc.stub.StreamObserver;
import io.grpc.StatusException;

@RunWith(JUnit4.class)
@SuppressWarnings({"unchecked", "rawtypes"})
public class TestBigtableTableAdminGrpcClient {

  // Class that implements a consistency service that returns dummy tokens and returns consistent
  // after a configurable number of calls.
  private static class ConsistencyServiceImpl extends BigtableTableAdminGrpc.BigtableTableAdminImplBase {

    // How many calls need to be done before tokens are consistent.
    private int callsToConsistency = 0;

    // Number of calls done so far.
    private int calls = 0;

    public void setCallsToConsistency(int callsToConsistency) {
      this.callsToConsistency = callsToConsistency;
    }

    @Override
    public void generateConsistencyToken(GenerateConsistencyTokenRequest request,
        StreamObserver<GenerateConsistencyTokenResponse> responseObserver) {

      // Generates a token like "TokenFor-projects/P/instances/I/tables/T"
      GenerateConsistencyTokenResponse response =
          GenerateConsistencyTokenResponse.newBuilder()
              .setConsistencyToken("TokenFor-" + request.getName())
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }

    @Override
    public void checkConsistency(CheckConsistencyRequest request,
        StreamObserver<CheckConsistencyResponse> responseObserver) {
      calls++;

      // Verify that we get the tokens we expect.
      if (!request.getConsistencyToken().equals("TokenFor-" + request.getName())) {
        responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT));
        return;

      }

      CheckConsistencyResponse response = CheckConsistencyResponse.newBuilder()
              .setConsistent(calls >= callsToConsistency)
              .build();

      responseObserver.onNext(response);
      responseObserver.onCompleted();
    }
  };

  @Rule
  public final GrpcServerRule grpcServerRule = new GrpcServerRule();

  private static final BigtableTableName TABLE_NAME =
      new BigtableTableName("projects/SomeProject/instances/SomeInstance/tables/SomeTable");

  private ConsistencyServiceImpl service;
  private BigtableTableAdminGrpcClient tableAdminClient;
  private MockBackOff backoff;

  @Before
  public void setup() {
    service = new ConsistencyServiceImpl();
    grpcServerRule.getServiceRegistry().addService(service);

    BigtableOptions options = new BigtableOptions.Builder().build();

    tableAdminClient = new BigtableTableAdminGrpcClient(grpcServerRule.getChannel(), null, options);
    backoff = new MockBackOff();
  }

  @Test
  public void testConsistencyFast() throws Exception {
    // Consistent right away.
    service.setCallsToConsistency(1);
    backoff.setMaxTries(10);
    tableAdminClient.waitForReplication(TABLE_NAME, backoff);
    Assert.assertEquals(0, backoff.getNumberOfTries());
  }

  @Test
  public void testConsistencySlow() throws Exception {
    // Consistent after a few calls.
    service.setCallsToConsistency(10);
    backoff.setMaxTries(9);
    tableAdminClient.waitForReplication(TABLE_NAME, backoff);
    Assert.assertEquals(9, backoff.getNumberOfTries());
  }

  @Test(expected = TimeoutException.class)
  public void testConsistencyTimeOut() throws Exception {
    // Time outs before consistency.
    service.setCallsToConsistency(10);
    backoff.setMaxTries(8);
    tableAdminClient.waitForReplication(TABLE_NAME, backoff);
  }
}
