/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.adapters.read;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import org.apache.hadoop.hbase.client.Operation;

/**
 * Interface used for Scan and Get operation adapters.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public interface ReadOperationAdapter<T extends Operation> {
  /**
   * adapt.
   *
   * @param request a T object.
   * @param readHooks a {@link ReadHooks} object.
   * @param query a {@link Query} object.
   */
  Query adapt(T request, ReadHooks readHooks, Query query);
}
