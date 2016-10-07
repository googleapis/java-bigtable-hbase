/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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


import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

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
        RowFilter.newBuilder()
            .setRowKeyRegexFilter(
                ByteString.copyFrom(prefixRegex))
            .build(),
        adapter.adapt(context, filter));
  }
}
