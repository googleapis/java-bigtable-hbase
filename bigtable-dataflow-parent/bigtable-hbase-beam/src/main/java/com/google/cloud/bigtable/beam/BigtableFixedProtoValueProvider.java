/*
 * Copyright 2023 Google LLC
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

import com.google.bigtable.repackaged.com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.repackaged.com.google.bigtable.v2.TableName;
import com.google.cloud.bigtable.hbase.BigtableFixedProtoScan;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.hadoop.hbase.client.Scan;

/** A {@link ValueProvider} class constructed from value providers. */
class BigtableFixedProtoValueProvider implements ValueProvider<Scan> {

  private ValueProvider<ReadRowsRequest> request;
  @Nullable private ValueProvider<String> projectId;
  @Nullable private ValueProvider<String> instanceId;
  @Nullable private ValueProvider<String> tableId;

  BigtableFixedProtoValueProvider(ValueProvider<ReadRowsRequest> request) {
    this.request = request;
  }

  BigtableFixedProtoValueProvider(
      ValueProvider<String> projectId,
      ValueProvider<String> instanceId,
      ValueProvider<String> tableId,
      ReadRowsRequest request) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.tableId = tableId;
    this.request = StaticValueProvider.of(request);
  }

  /** Returns the {@link ReadRowsRequest}. */
  ReadRowsRequest getRequest() {
    if (projectId != null && instanceId != null && tableId != null) {
      ReadRowsRequest readRowsRequest =
          request
              .get()
              .toBuilder()
              .setTableName(TableName.format(projectId.get(), instanceId.get(), tableId.get()))
              .build();
      return readRowsRequest;
    } else {
      return request.get();
    }
  }

  /** Wraps the {@link ReadRowsRequest} in a {@link BigtableFixedProtoScan}. */
  @Override
  public Scan get() {
    return new BigtableFixedProtoScan(getRequest());
  }

  /** Returns true if the {@link ReadRowsRequest} is accessible. */
  @Override
  public boolean isAccessible() {
    if (projectId != null && instanceId != null && tableId != null) {
      return projectId.isAccessible()
          && instanceId.isAccessible()
          && tableId.isAccessible()
          && request.isAccessible();
    } else {
      return request.isAccessible();
    }
  }
}
