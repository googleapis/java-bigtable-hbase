/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase.adapters;

import com.google.cloud.hadoop.hbase.repackaged.protobuf.GeneratedMessage;

import org.apache.hadoop.hbase.client.Result;

/**
 * An adapter for transforming a response from the Anviltop server to a HBase result.
 * @param <T> The response type from Anviltop
 * @param <U> The HBase result type
 */
public interface ResponseAdapter<T extends GeneratedMessage, U extends Result> {

  /**
   * Transform an Anviltop server response to an HBase Result instance.
   * @param response The anviltop response to transform.
   */
  public U adaptResponse(T response);
}
