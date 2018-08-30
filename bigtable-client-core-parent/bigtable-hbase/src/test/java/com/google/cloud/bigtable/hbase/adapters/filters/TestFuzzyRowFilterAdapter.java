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
package com.google.cloud.bigtable.hbase.adapters.filters;

import static com.google.cloud.bigtable.data.v2.wrappers.Filters.FILTERS;

import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowFilter.Interleave;
import com.google.cloud.bigtable.data.v2.wrappers.Filters;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;

@RunWith(JUnit4.class) public class TestFuzzyRowFilterAdapter {
  FuzzyRowFilterAdapter adapter = new FuzzyRowFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext context = new FilterAdapterContext(emptyScan, null);

  @Test public void fuzzyKeysAreTranslatedToRegularExpressions() throws IOException {
    List<Pair<byte[], byte[]>> testPairs = ImmutableList.<Pair<byte[], byte[]>>builder()
        .add(new Pair<>(Bytes.toBytes("abcd"), new byte[] { -1, -1, -1, -1 }))
        .add(new Pair<>(Bytes.toBytes(".fgh"), new byte[] { 0, 0, 1, 0 }))
        .add(new Pair<>(Bytes.toBytes("ijkl"), new byte[] { 1, 1, 1, 1 }))
        .build();

    FuzzyRowFilter filter = new FuzzyRowFilter(testPairs);
    RowFilter adaptedFilter = adapter.adapt(context, filter);
    RowFilter expected = FILTERS.interleave()
        .filter(FILTERS.key().regex("abcd\\C*"))
        .filter(FILTERS.key().regex("\\.f\\Ch\\C*"))
        .filter(FILTERS.key().regex("\\C\\C\\C\\C\\C*"))
        .toProto();


    Assert.assertEquals(expected, adaptedFilter);
  }
}