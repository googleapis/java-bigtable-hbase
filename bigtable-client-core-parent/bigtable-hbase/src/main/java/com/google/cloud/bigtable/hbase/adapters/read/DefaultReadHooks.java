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
import com.google.common.base.Functions;

/**
 * Default implementation of {@link ReadHooks}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class DefaultReadHooks implements ReadHooks {
  private Function<Query, Query> preSendHook = Functions.identity();

  /** {@inheritDoc} */
  @Override
  public void composePreSendHook(Function<Query, Query> newHook) {
    preSendHook = Functions.compose(newHook, preSendHook);
  }

  /** {@inheritDoc} */
  @Override
  public void applyPreSendHook(Query query) {
    preSendHook.apply(query);
  }
}
