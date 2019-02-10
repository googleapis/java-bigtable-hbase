/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.cloud.bigtable.data.v2.models.MutationApi;
import org.apache.hadoop.hbase.client.Row;

import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.protobuf.ByteString;

/**
 * Adapt an HBase {@link Row} Operation into a Google Cloud Java
 * {@link MutationApi}.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public abstract class MutationAdapter<T extends Row>
    implements OperationAdapter<T, MutationApi<?>> {

  protected static byte[] getBytes(ByteString bs) {
    return ByteStringer.extract(bs);
  }

  /**
   * Converts an HBase {@link Row} which represents a set of changes to a single row from an
   * HBase perspective to a Google Cloud Java {@link MutationApi}
   * which represent the set of changes.
   *
   * @param operation The HBase {@link Row} to convert
   * @param mutation The model {@link MutationApi}
   */
  @Override
  public abstract void adapt(T operation, MutationApi<?> mutation);
}
