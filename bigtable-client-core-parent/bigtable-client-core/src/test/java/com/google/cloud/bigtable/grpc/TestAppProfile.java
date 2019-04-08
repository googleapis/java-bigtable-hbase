/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v2.BigtableGrpc.BigtableImplBase;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.CheckAndMutateRowResponse;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.v2.MutateRowsResponse.Entry;
import com.google.bigtable.v2.ReadModifyWriteRowRequest;
import com.google.bigtable.v2.ReadModifyWriteRowResponse;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.core.IBulkMutation;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.ReadModifyWriteRow;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.common.base.Preconditions;
import com.google.common.collect.Queues;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAppProfile {
  private static final String TABLE_ID = "fake-table";

  private FakeDataService fakeDataService;
  private Server server;

  private BigtableSession defaultSession;
  private BigtableSession profileSession;

  @Before
  public void setUp() throws IOException {
    fakeDataService = new FakeDataService();

    final int port;
    try(ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    server = ServerBuilder.forPort(port)
        .addService(fakeDataService)
        .build();
    server.start();


    BigtableOptions opts = BigtableOptions.builder()
        .setDataHost("localhost")
        .setAdminHost("locahost")
        .setPort(port)
        .setProjectId("fake-project")
        .setInstanceId("fake-instance")
        .setUserAgent("fake-agent")
        .setUsePlaintextNegotiation(true)
        .setCredentialOptions(CredentialOptions.nullCredential())
        .build();

    defaultSession = new BigtableSession(opts);

    profileSession = new BigtableSession(
        opts.toBuilder()
        .setAppProfileId("my-app-profile")
        .build()
    );
  }

  @After
  public void tearDown() throws Exception {
    if (defaultSession != null) {
      defaultSession.close();
    }

    if (profileSession != null) {
      profileSession.close();
    }

    if (server != null) {
      server.shutdownNow();
      server.awaitTermination();
    }
  }

  @Test
  public void testReadRows() throws Exception {
    defaultSession.getDataClientWrapper().readRows(Query.create(TABLE_ID)).next();
    ReadRowsRequest req = fakeDataService.popLastRequest();
    Preconditions.checkState(req.getAppProfileId().isEmpty());

    profileSession.getDataClientWrapper().readRows(Query.create(TABLE_ID));
    ReadRowsRequest req2 = fakeDataService.popLastRequest();
    Assert.assertEquals(req2.getAppProfileId(), "my-app-profile");
  }

  @Test
  public void testSampleRowKeys() throws Exception {
    defaultSession.getDataClientWrapper().sampleRowKeys(TABLE_ID);
    SampleRowKeysRequest req = fakeDataService.popLastRequest();
    Preconditions.checkState(req.getAppProfileId().isEmpty());

    profileSession.getDataClientWrapper().sampleRowKeys(TABLE_ID);
    SampleRowKeysRequest req2 = fakeDataService.popLastRequest();
    Assert.assertEquals(req2.getAppProfileId(), "my-app-profile");
  }

  @Test
  public void testMutateRow() throws Exception {
    RowMutation rowMutation = RowMutation.create(TABLE_ID, "fake-key");
    defaultSession.getDataClientWrapper().mutateRow(rowMutation);
    MutateRowRequest req = fakeDataService.popLastRequest();
    Preconditions.checkState(req.getAppProfileId().isEmpty());

    profileSession.getDataClientWrapper().mutateRow(rowMutation);
    MutateRowRequest req2 = fakeDataService.popLastRequest();
    Assert.assertEquals(req2.getAppProfileId(), "my-app-profile");

  }

  @Test
  public void testMutateRows() throws Exception {
    defaultSession.getDataClient().mutateRows(MutateRowsRequest.getDefaultInstance());
    MutateRowsRequest req = fakeDataService.popLastRequest();
    Preconditions.checkState(req.getAppProfileId().isEmpty());

    profileSession.getDataClient().mutateRows(MutateRowsRequest.getDefaultInstance());
    MutateRowsRequest req2 = fakeDataService.popLastRequest();
    Assert.assertEquals(req2.getAppProfileId(), "my-app-profile");
  }

  @Test
  public void testCheckAndMutateRow() throws Exception {
    ConditionalRowMutation checkAndMuate =
        ConditionalRowMutation.create(TABLE_ID, "fake-key")
            .then(Mutation.create()
                .setCell("fakeFamily", "qualifer", "value"));
    defaultSession.getDataClientWrapper().checkAndMutateRow(checkAndMuate);
    CheckAndMutateRowRequest req = fakeDataService.popLastRequest();
    Preconditions.checkState(req.getAppProfileId().isEmpty());

    profileSession.getDataClientWrapper().checkAndMutateRow(checkAndMuate);
    CheckAndMutateRowRequest req2 = fakeDataService.popLastRequest();
    Assert.assertEquals(req2.getAppProfileId(), "my-app-profile");
  }

  @Test
  public void testReadModifyWrite() throws Exception {
    ReadModifyWriteRow readModifyRow = ReadModifyWriteRow.create(TABLE_ID, "fake-key");
    defaultSession.getDataClientWrapper().readModifyWriteRow(readModifyRow);
    ReadModifyWriteRowRequest req = fakeDataService.popLastRequest();
    Preconditions.checkState(req.getAppProfileId().isEmpty());

    profileSession.getDataClientWrapper().readModifyWriteRow(readModifyRow);
    ReadModifyWriteRowRequest req2 = fakeDataService.popLastRequest();
    Assert.assertEquals(req2.getAppProfileId(), "my-app-profile");
  }

  @Test
  public void testBulkMutation() throws Exception {
    BigtableTableName fakeTableName = new BigtableTableName(
        "projects/fake-project/instances/fake-instance/tables/fake-table");

    RowMutation rowMutation = RowMutation.create(TABLE_ID, "fake-key");

    IBulkMutation bulkMutation = defaultSession.createBulkMutationWrapper(fakeTableName);
    bulkMutation.add(rowMutation);
    bulkMutation.flush();

    MutateRowsRequest req = fakeDataService.popLastRequest();
    Preconditions.checkState(req.getAppProfileId().isEmpty());

    IBulkMutation bulkMutation2 = profileSession.createBulkMutationWrapper(fakeTableName);
    bulkMutation2.add(rowMutation);
    bulkMutation2.flush();

    MutateRowsRequest req2 = fakeDataService.popLastRequest();
    Assert.assertEquals(req2.getAppProfileId(), "my-app-profile");
  }

  @Test
  public void testBulkRead() throws Exception {
    String tableId = "fake-table";
    BigtableTableName fakeTableName =
        defaultSession.getOptions().getInstanceName().toTableName(tableId);

    Query readRowsRequest = Query.create(tableId).rowKey("fake-key");

    BulkRead bulkRead = defaultSession.createBulkRead(fakeTableName);
    bulkRead.add(readRowsRequest);
    bulkRead.flush();

    ReadRowsRequest req = fakeDataService.popLastRequest();
    Preconditions.checkState(req.getAppProfileId().isEmpty());

    BulkRead bulkRead2 = profileSession.createBulkRead(fakeTableName);
    bulkRead2.add(readRowsRequest);
    bulkRead2.flush();

    ReadRowsRequest req2 = fakeDataService.popLastRequest();
    Assert.assertEquals(req2.getAppProfileId(), "my-app-profile");
  }

  static class FakeDataService extends BigtableImplBase {
    final BlockingQueue<Object> requests = Queues.newLinkedBlockingDeque();

    @SuppressWarnings("unchecked")
    <T> T popLastRequest() throws InterruptedException {
      return (T)requests.poll(1, TimeUnit.SECONDS);
    }

    @Override
    public void readRows(ReadRowsRequest request,
        StreamObserver<ReadRowsResponse> responseObserver) {
      requests.add(request);
      responseObserver.onCompleted();
    }

    @Override
    public void sampleRowKeys(SampleRowKeysRequest request,
        StreamObserver<SampleRowKeysResponse> responseObserver) {

      requests.add(request);
      responseObserver.onCompleted();
    }

    @Override
    public void mutateRow(MutateRowRequest request,
        StreamObserver<MutateRowResponse> responseObserver) {

      requests.add(request);
      responseObserver.onNext(MutateRowResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void mutateRows(MutateRowsRequest request,
        StreamObserver<MutateRowsResponse> responseObserver) {

      requests.add(request);
      MutateRowsResponse.Builder response = MutateRowsResponse.newBuilder();
      for (int i = 0; i < request.getEntriesCount(); i++) {
        response.addEntries(
            Entry.newBuilder()
                .setIndex(i)
                .setStatus(Status.newBuilder().setCode(Code.OK.getNumber()).build())
                .build()
        );
      }
      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void checkAndMutateRow(CheckAndMutateRowRequest request,
        StreamObserver<CheckAndMutateRowResponse> responseObserver) {

      requests.add(request);
      CheckAndMutateRowResponse.Builder response = CheckAndMutateRowResponse.newBuilder()
          .setPredicateMatched(false);

      responseObserver.onNext(response.build());
      responseObserver.onCompleted();
    }

    @Override
    public void readModifyWriteRow(ReadModifyWriteRowRequest request,
        StreamObserver<ReadModifyWriteRowResponse> responseObserver) {

      requests.add(request);
      responseObserver.onNext(ReadModifyWriteRowResponse.getDefaultInstance());
      responseObserver.onCompleted();
    }
  }
}
