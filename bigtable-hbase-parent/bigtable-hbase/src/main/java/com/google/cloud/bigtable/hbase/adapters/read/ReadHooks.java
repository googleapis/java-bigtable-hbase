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
package com.google.cloud.bigtable.hbase.adapters.read;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.common.base.Function;

/**
 * Hooks for modifying a ReadRowsRequest before being sent to Cloud Bigtable.
 *
 * Note that it is expected that this will be extended to include post-read
 * hooks to transform Rows when appropriate.
 */
public interface ReadHooks {

  /**
   * Add a Function that will modify the ReadRowsRequest before it is sent to Cloud Bigtable.
   */
  void composePreSendHook(Function<ReadRowsRequest, ReadRowsRequest> newHook);

  /**
   * Apply all pre-send hooks to the given request.
   */
  ReadRowsRequest applyPreSendHook(ReadRowsRequest readRowsRequest);
}
