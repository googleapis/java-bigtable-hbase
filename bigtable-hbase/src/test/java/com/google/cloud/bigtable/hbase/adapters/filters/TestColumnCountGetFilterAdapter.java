/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

@RunWith(JUnit4.class)
public class TestColumnCountGetFilterAdapter {

  ColumnCountGetFilterAdapter adapter = new ColumnCountGetFilterAdapter();

  @Test
  public void testSimpleColumnCount() throws IOException {
    RowFilter adaptedFilter =
        adapter.adapt(
            new FilterAdapterContext(new Scan()),
            new ColumnCountGetFilter(2));
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setChain(Chain.newBuilder()
                .addFilters(
                    RowFilter.newBuilder()
                        .setCellsPerColumnLimitFilter(1))
                .addFilters(
                    RowFilter.newBuilder()
                        .setCellsPerRowLimitFilter(2)))
        .build(),
        adaptedFilter);
  }
}
