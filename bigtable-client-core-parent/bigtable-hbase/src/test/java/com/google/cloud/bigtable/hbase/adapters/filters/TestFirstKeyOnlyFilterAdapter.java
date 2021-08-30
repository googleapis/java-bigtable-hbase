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
package com.google.cloud.bigtable.hbase.adapters.filters;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.cloud.bigtable.data.v2.models.Filters;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestFirstKeyOnlyFilterAdapter {

  private static final FirstKeyOnlyFilterAdapter adapter = new FirstKeyOnlyFilterAdapter();

  @Test
  public void onlyTheFirstKeyFromEachRowIsEmitted() throws IOException {
    Filters.Filter adaptedFilter =
        adapter.adapt(new FilterAdapterContext(new Scan(), null), new FirstKeyOnlyFilter());
    Assert.assertEquals(FILTERS.limit().cellsPerRow(1).toProto(), adaptedFilter.toProto());
  }
}
