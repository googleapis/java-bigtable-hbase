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

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SimpleIT {
  @Test
  public void testConnection() throws Exception {
    String projectId = System.getProperty("google.bigtable.project.id");
    String instanceId = System.getProperty("google.bigtable.instance.id");
    String tableId = "myTable";
    String family = "cf";
    String key = "key";
    String value = "value";

    String instanceName = "projects/" + projectId + "/instances/" + instanceId;
    String tableName = instanceName + "/tables/" + tableId;

    BigtableOptions opts = new BigtableOptions.Builder()
        .setUserAgent("fake")
        .setCredentialOptions(CredentialOptions.nullCredential())
        .setUsePlaintextNegotiation(true)
        .setPort(Integer.parseInt(System.getProperty("google.bigtable.endpoint.port")))
        .setDataHost(System.getProperty("google.bigtable.endpoint.host"))
        .setInstanceAdminHost(System.getProperty("google.bigtable.instance.admin.endpoint.host"))
        .setTableAdminHost(System.getProperty("google.bigtable.admin.endpoint.host"))
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build();

    try (BigtableSession session = new BigtableSession(opts)) {
      BigtableTableAdminClient tableAdminClient = session.getTableAdminClient();
      tableAdminClient.createTable(
          CreateTableRequest.newBuilder()
              .setParent(instanceName)
              .setTableId(tableId)
              .setTable(
                  Table.newBuilder()
                      .putColumnFamilies(family, ColumnFamily.getDefaultInstance())
              )
              .build()
      );

      BigtableDataClient dataClient = session.getDataClient();
      dataClient.mutateRow(
          MutateRowRequest.newBuilder()
              .setTableName(tableName)
              .setRowKey(ByteString.copyFromUtf8(key))
              .addMutations(
                  Mutation.newBuilder()
                      .setSetCell(
                          SetCell.newBuilder()
                              .setFamilyName(family)
                              .setValue(ByteString.copyFromUtf8(value))
                      )
              )
              .build()
      );

      List<Row> results = dataClient.readRowsAsync(ReadRowsRequest.newBuilder()
          .setTableName(tableName)
          .setRows(
              RowSet.newBuilder()
                  .addRowKeys(ByteString.copyFromUtf8(key))
          )
          .build()
      ).get();

      Assert.assertEquals(ByteString.copyFromUtf8(value), results.get(0).getFamilies(0).getColumns(0).getCells(0).getValue());
    }
  }
}
