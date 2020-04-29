/*
 * Copyright 2020 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase1_x;

import static org.junit.Assert.assertEquals;

import com.google.bigtable.admin.v2.BigtableTableAdminGrpc;
import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.*;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigtableAdminTest {
  private static final String PROJECT_ID = "fake-project";
  private static final String INSTANCE_ID = "fake-instance";
  private static final String TABLE_ID = "fake-table";
  private static final String TABLE_NAME =
      String.format("projects/%s/instances/%s/tables/%s", PROJECT_ID, INSTANCE_ID, TABLE_ID);

  private Server fakeBigtableServer;
  private BigtableConnection connection;
  private BigtableAdmin admin;

  private ArrayBlockingQueue requestQueue = new ArrayBlockingQueue(1);
  private ArrayBlockingQueue responseQueue = new ArrayBlockingQueue(1);

  @Before
  public void setup() throws Exception {
    final int port;

    try (ServerSocket serverSocket = new ServerSocket(0)) {
      port = serverSocket.getLocalPort();
    }

    fakeBigtableServer =
        ServerBuilder.forPort(port)
            .intercept(new RequestInterceptor())
            .addService(new BigtableTableAdminGrpc.BigtableTableAdminImplBase() {})
            .build();
    fakeBigtableServer.start();

    Configuration configuration = BigtableConfiguration.configure(PROJECT_ID, INSTANCE_ID);
    configuration.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:" + port);
    connection = new BigtableConnection(configuration);
    admin = (BigtableAdmin) connection.getAdmin();
  }

  @After
  public void teardown() throws IOException {
    connection.close();
    fakeBigtableServer.shutdown();
  }

  @Test
  public void testDeleteRowRangeByPrefixNonUtf8() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf(TABLE_ID);
    ByteString expectedKey = ByteString.copyFrom(new byte[] {0, 0, 0, (byte) 128});

    DropRowRangeRequest expectedRequest =
        DropRowRangeRequest.newBuilder().setName(TABLE_NAME).setRowKeyPrefix(expectedKey).build();
    responseQueue.put(Empty.getDefaultInstance());

    admin.deleteRowRangeByPrefix(tableName, expectedKey.toByteArray());

    assertEquals(expectedRequest, requestQueue.take());
  }

  private class RequestInterceptor implements ServerInterceptor {
    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
        final ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
      return new ServerCall.Listener<ReqT>() {
        @Override
        public void onReady() {
          call.request(1);
        }

        @Override
        public void onMessage(ReqT message) {
          requestQueue.add(message);

          call.sendHeaders(new Metadata());
          try {
            @SuppressWarnings("unchecked")
            RespT response = (RespT) responseQueue.take();
            call.sendMessage(response);
            call.close(Status.OK, new Metadata());
          } catch (InterruptedException e) {
            call.close(
                Status.CANCELLED
                    .withCause(e)
                    .withDescription("Timed out waiting for mock response"),
                new Metadata());
          }
        }
      };
    }
  }
}
