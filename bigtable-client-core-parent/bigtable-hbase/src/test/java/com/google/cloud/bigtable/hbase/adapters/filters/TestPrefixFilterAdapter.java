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


import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.protobuf.ByteString;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link PrefixFilterAdapter}
 */
@RunWith(JUnit4.class)
public class TestPrefixFilterAdapter {

  @Test
  public void testPrefixAddedAsRowRegex() throws IOException {
    PrefixFilterAdapter adapter = new PrefixFilterAdapter();
    String prefix = "Foobar";
    PrefixFilter filter = new PrefixFilter(Bytes.toBytes(prefix));
    Scan emptyScan = new Scan();
    FilterAdapterContext context = new FilterAdapterContext(emptyScan, null);

    byte[] prefixRegex = Bytes.toBytes(prefix + "\\C*");
    Assert.assertEquals(
      FILTERS.key()
        .regex(ByteString.copyFrom(prefixRegex)).toProto(),
        adapter.adapt(context, filter).toProto());
  }

  @Test
  public void testIndexIsNarrowed() {
    PrefixFilterAdapter adapter = new PrefixFilterAdapter();
    String prefix = "Foobar";
    PrefixFilter filter = new PrefixFilter(Bytes.toBytes(prefix));

    RangeSet<RowKeyWrapper> hint = adapter.getIndexScanHint(filter);

    ImmutableRangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(
            Range.closedOpen(
                new RowKeyWrapper(ByteString.copyFromUtf8("Foobar")),
                new RowKeyWrapper(ByteString.copyFromUtf8("Foobas"))
            ))
        .build();

    Assert.assertEquals(expected, hint);
  }
}
