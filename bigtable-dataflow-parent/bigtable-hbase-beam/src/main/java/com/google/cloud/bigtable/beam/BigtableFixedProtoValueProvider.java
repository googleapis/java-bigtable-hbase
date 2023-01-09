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
import com.google.cloud.bigtable.hbase.BigtableFixedProtoScan;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.hadoop.hbase.client.Scan;

/** A {@link ValueProvider} class constructed from a {@link ReadRowsRequest} value provider. */
class BigtableFixedProtoValueProvider implements ValueProvider<Scan> {

  private ValueProvider<ReadRowsRequest> request;

  BigtableFixedProtoValueProvider(ValueProvider<ReadRowsRequest> request) {
    this.request = request;
  }

  /** Returns the {@link ReadRowsRequest}. */
  ReadRowsRequest getRequest() {
    return request.get();
  }

  /** Wraps the {@link ReadRowsRequest} in a {@link BigtableFixedProtoScan}. */
  @Override
  public Scan get() {
    return new BigtableFixedProtoScan(request.get());
  }

  /** Returns true if the {@link ReadRowsRequest} is accessible. */
  @Override
  public boolean isAccessible() {
    return request.isAccessible();
  }
}
