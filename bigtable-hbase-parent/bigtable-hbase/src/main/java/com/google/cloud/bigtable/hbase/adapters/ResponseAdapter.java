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

import org.apache.hadoop.hbase.client.Result;

/**
 * An adapter for transforming a response from the Bigtable server to a HBase result.
 *
 * @param <T> The response type from Bigtable
 * @param <U> The HBase result type
 * @author sduskis
 * @version $Id: $Id
 */
public interface ResponseAdapter<T, U extends Result> {

  /**
   * Transform an Bigtable server response to an HBase Result instance.
   *
   * @param response The Bigtable response to transform.
   * @return a U object.
   */
  public U adaptResponse(T response);
}
