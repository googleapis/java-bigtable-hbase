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
import com.google.common.base.Functions;

/**
 * Default implementation of {@link com.google.cloud.bigtable.hbase.adapters.read.ReadHooks}.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class DefaultReadHooks implements ReadHooks {
  private Function<ReadRowsRequest, ReadRowsRequest> preSendHook = Functions.identity();
  /** {@inheritDoc} */
  @Override
  public void composePreSendHook(Function<ReadRowsRequest, ReadRowsRequest> newHook) {
    preSendHook = Functions.compose(newHook, preSendHook);
  }

  /** {@inheritDoc} */
  @Override
  public ReadRowsRequest applyPreSendHook(ReadRowsRequest readRowsRequest) {
    return preSendHook.apply(readRowsRequest);
  }
}
