/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.repackaged.com.google.api.client.util.Clock;
import com.google.cloud.bigtable.dataflow.coders.HBaseMutationCoderTest;
import com.google.cloud.bigtable.hbase.adapters.PutAdapter;

/**
 * The {@link PutAdapter}'s {@link Clock} is package private. This utility in the same package
 * allows {@link HBaseMutationCoderTest} set the package private clock for testing purposes.
 */
public final class PutAdapterUtil {

  /**
   * Sets a package private clock variable on the {@link PutAdapter} instance.
   *
   * @param putAdapter The {@link PutAdapter} on which to set the {@link Clock}.
   * @param clock The {@link Clock} to set in the {@link PutAdapter}.
   */
  public static void setClock(PutAdapter putAdapter, Clock clock){
    putAdapter.clock = clock;
  }

  private PutAdapterUtil(){}
}
