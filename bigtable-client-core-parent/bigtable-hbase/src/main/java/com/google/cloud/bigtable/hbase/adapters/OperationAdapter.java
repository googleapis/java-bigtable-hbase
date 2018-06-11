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

import com.google.protobuf.MessageLite;

import org.apache.hadoop.hbase.client.Row;

/**
 * An interface for adapters that will convert an HBase Operation into an Bigtable
 *
 * @param <T> The HBase operation type
 * @param <U> The Bigtable message type
 * @author sduskis
 * @version $Id: $Id
 */
public interface OperationAdapter<T extends Row, U extends MessageLite.Builder> {

  /**
   * Adapt a single HBase Operation to a single Bigtable generated message.
   *
   * @param operation The HBase operation to convert.
   * @return An equivalent Bigtable
   */
  public U adapt(T operation);
}
