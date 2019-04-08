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

import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.core.IBigtableDataClient;
import com.google.cloud.bigtable.core.IBigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SimpleIT {
  @Test
  public void testConnection() throws Exception {
    String projectId = "fake-project";
    String instanceId = "fake-instance";
    String tableId = "myTable";
    String family = "cf";
    String key = "key";
    String value = "value";

    BigtableOptions opts = new BigtableOptions.Builder()
        .setUserAgent("fake")
        .setProjectId(projectId)
        .setInstanceId(instanceId)
        .build();

    try (BigtableSession session = new BigtableSession(opts)) {
      IBigtableTableAdminClient tableAdminClient = session.getTableAdminClientWrapper();
      tableAdminClient.createTable(CreateTableRequest.of(tableId).addFamily(family));

      IBigtableDataClient dataClient = session.getDataClientWrapper();
      dataClient.mutateRow(RowMutation.create(tableId, key).setCell(family, "", value));
      List<Row> results = dataClient.readRowsAsync(Query.create(tableId).rowKey(key)).get();

      Assert.assertEquals(ByteString.copyFromUtf8(value), results.get(0).getCells().get(0).getValue());
    }
  }
}

