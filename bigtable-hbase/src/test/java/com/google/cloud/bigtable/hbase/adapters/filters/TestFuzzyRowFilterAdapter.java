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
import com.google.bigtable.v1.RowFilter.Interleave;
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

@RunWith(JUnit4.class)
public class TestFuzzyRowFilterAdapter {
  FuzzyRowFilterAdapter adapter = new FuzzyRowFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext context = new FilterAdapterContext(emptyScan);

  @Test
  public void fuzzyKeysAreTranslatedToRegularExpressions() throws IOException {
    List<Pair<byte[], byte[]>> testPairs =
        ImmutableList.<Pair<byte[], byte[]>>builder()
            .add(new Pair<>(new byte[]{0, 0, 0, 0}, Bytes.toBytes("abcd")))
            .add(new Pair<>(new byte[]{0, 0, 1, 0}, Bytes.toBytes(".fgh")))
            .add(new Pair<>(new byte[]{1, 1, 1, 1}, Bytes.toBytes("ijkl")))
        .build();

    FuzzyRowFilter filter = new FuzzyRowFilter(testPairs);
    RowFilter adaptedFilter = adapter.adapt(context, filter);
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setInterleave(
                Interleave.newBuilder()
                    .addFilters(
                        RowFilter.newBuilder()
                            .setRowKeyRegexFilter(
                                ByteString.copyFromUtf8("abcd")))
                    .addFilters(
                        RowFilter.newBuilder()
                            .setRowKeyRegexFilter(
                                ByteString.copyFromUtf8("\\.f\\Ch")))
                    .addFilters(
                        RowFilter.newBuilder()
                            .setRowKeyRegexFilter(
                                ByteString.copyFromUtf8("\\C\\C\\C\\C"))))
        .build(),
        adaptedFilter);
  }
}
