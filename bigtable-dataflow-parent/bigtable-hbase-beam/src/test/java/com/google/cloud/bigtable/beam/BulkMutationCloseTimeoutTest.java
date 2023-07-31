/*
 * Copyright 2017 Google Inc.
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
package com.google.cloud.bigtable.beam;

import com.google.bigtable.repackaged.com.google.bigtable.v2.BigtableGrpc.BigtableImplBase;
import com.google.bigtable.repackaged.com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.repackaged.com.google.bigtable.v2.MutateRowsResponse;
import com.google.bigtable.repackaged.io.grpc.ServerBuilder;
import com.google.bigtable.repackaged.io.grpc.stub.StreamObserver;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.cloud.bigtable.hbase.wrappers.BigtableHBaseSettings;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class BulkMutationCloseTimeoutTest {

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public final TemporaryFolder workDir = new TemporaryFolder();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test(timeout = 20000)
  public void testBulkMutationCloseTimeout() throws Throwable {
    thrown.expect(PipelineExecutionException.class);
    thrown.expectMessage("Cloud not close the bulk mutation Batcher, timed out in close()");

    int port = startFakeBigtableService();

    List<Mutation> data = new ArrayList<>();
    for (int i = 0; i < 1; i++) {
      Put row = new Put(Bytes.toBytes("key-123"));
      row.addColumn(
          Bytes.toBytes("column-family"), Bytes.toBytes("column"), Bytes.toBytes("value"));
      data.add(row);
    }

    CloudBigtableTableConfiguration config =
        new CloudBigtableTableConfiguration.Builder()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withTableId("test-table")
            .withConfiguration(
                BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + port)
            .withConfiguration(
                BigtableHBaseSettings.BULK_MUTATION_CLOSE_TIMEOUT_MILLISECONDS, "10000")
            .build();

    CloudBigtableIO.writeToTable(config);
    writePipeline.apply(Create.of(data)).apply(CloudBigtableIO.writeToTable(config));
    writePipeline.run().waitUntilFinish();
  }

  private int startFakeBigtableService() throws IOException {
    int port;
    try (ServerSocket ss = new ServerSocket(0)) {
      port = ss.getLocalPort();
    }
    ServerBuilder builder = ServerBuilder.forPort(port);
    builder.addService(new FakeBigtableService());
    builder.build().start();

    System.out.println("Starting FakeBigtableService on port: " + port);
    return port;
  }

  public static class FakeBigtableService extends BigtableImplBase {

    @Override
    public void mutateRows(MutateRowsRequest request, StreamObserver<MutateRowsResponse> observer) {
      // This test intentionally skips calling observer.onCompleted() to this operation hang.
    }
  }
}
