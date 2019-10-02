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

import com.google.api.core.InternalApi;
import org.apache.hadoop.hbase.client.Row;

/**
 * An interface for adapters that will convert an HBase Operation into Google Cloud Java Bigtable
 * Models type.
 *
 * <p>For internal use only - public for technical reasons.
 *
 * @param <T> The HBase operation type
 * @param <U> The Google Cloud Java Bigtable Model type.
 */
@InternalApi("For internal usage only")
public interface OperationAdapter<T extends Row, U> {

  /**
   * Adapt a single HBase Operation to a single Bigtable generated message.
   *
   * @param operation The HBase operation to convert.
   * @param u Type to which HBase operation will be mapped to. Typically it will be Google Cloud
   *     Java Bigtable Models.
   */
  public void adapt(T operation, U u);
}
