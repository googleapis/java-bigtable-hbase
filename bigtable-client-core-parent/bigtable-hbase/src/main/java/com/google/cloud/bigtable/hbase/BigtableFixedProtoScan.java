/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.hbase;

import com.google.api.core.InternalApi;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.cloud.bigtable.data.v2.models.Query;
import org.apache.hadoop.hbase.client.Scan;

/** A wrapper class that wraps a Bigtable {@link Query}. */
@InternalApi
public class BigtableFixedProtoScan extends Scan {

  private ReadRowsRequest request;

  public BigtableFixedProtoScan(ReadRowsRequest request) {
    this.request = request;
  }

  public ReadRowsRequest getRequest() {
    return request;
  }

  public void setRequest(Query query) {
    this.request = request;
  }
}
