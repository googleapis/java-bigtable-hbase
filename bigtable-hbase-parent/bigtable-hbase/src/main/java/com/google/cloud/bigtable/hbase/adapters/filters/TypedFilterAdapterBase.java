/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.hadoop.hbase.filter.Filter;


/**
 * Base functionality for all filter adapters
 * @param <S>
 */
public abstract class TypedFilterAdapterBase<S extends Filter> implements TypedFilterAdapter<S> {
  @Override
  public RangeSet<RowKeyWrapper> getIndexScanHint(S filter) {
    return ImmutableRangeSet.of(Range.<RowKeyWrapper>all());
  }
}
