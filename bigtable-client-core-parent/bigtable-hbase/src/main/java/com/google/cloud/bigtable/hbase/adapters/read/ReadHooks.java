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
import com.google.common.base.Function;

/**
 * Hooks for modifying a {@link Query} before being sent to Cloud Bigtable.
 *
 * <p>Note that it is expected that this will be extended to include post-read hooks to transform
 * Rows when appropriate.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public interface ReadHooks {

  /**
   * Add a {@link Function} that will modify the {@link Query} before it is sent to Cloud Bigtable.
   *
   * @param newHook a {@link Function} object.
   */
  void composePreSendHook(Function<Query, Query> newHook);

  /**
   * Apply all pre-send hooks to the given request.
   *
   * @param query a {@link Query} object.
   */
  void applyPreSendHook(Query query);
}
