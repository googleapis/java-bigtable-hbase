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
package com.google.cloud.bigtable.dataflow;

import com.google.bigtable.repackaged.com.google.cloud.config.BigtableOptions;
import com.google.bigtable.repackaged.com.google.cloud.grpc.BigtableSession;
import com.google.bigtable.repackaged.com.google.cloud.grpc.BigtableTableName;
import com.google.bigtable.repackaged.com.google.com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.repackaged.com.google.com.google.bigtable.v2.SampleRowKeysResponse;
import java.io.IOException;
import java.util.List;

public class CloudBigtableServiceImpl implements CloudBigtableService {

  @Override
  public List<SampleRowKeysResponse> getSampleRowKeys(CloudBigtableTableConfiguration config)
      throws IOException {
    BigtableOptions bigtableOptions = config.toBigtableOptions();
    try (BigtableSession session = new BigtableSession(bigtableOptions)) {
      BigtableTableName tableName =
          bigtableOptions.getInstanceName().toTableName(config.getTableId());
      SampleRowKeysRequest request =
          SampleRowKeysRequest.newBuilder().setTableName(tableName.toString()).build();
      return session.getDataClient().sampleRowKeys(request);
    }
  }
}
