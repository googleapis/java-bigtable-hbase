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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestFuzzyRowFilterAdapter {
  FuzzyRowFilterAdapter adapter = new FuzzyRowFilterAdapter();
  Scan emptyScan = new Scan();
  FilterAdapterContext context = new FilterAdapterContext(emptyScan, null);

  @Test
  public void fuzzyKeysAreTranslatedToRegularExpressions() throws IOException {
    List<Pair<byte[], byte[]>> testPairs =
        ImmutableList.<Pair<byte[], byte[]>>builder()
            .add(new Pair<>(Bytes.toBytes("abcd"), new byte[] {-1, -1, -1, -1}))
            .add(new Pair<>(Bytes.toBytes(".fgh"), new byte[] {0, 0, 1, 0}))
            .add(new Pair<>(Bytes.toBytes("ijkl"), new byte[] {1, 1, 1, 1}))
            .build();

    FuzzyRowFilter filter = new FuzzyRowFilter(testPairs);
    Filters.Filter adaptedFilter = adapter.adapt(context, filter);
    Filters.Filter expected =
        FILTERS
            .interleave()
            .filter(FILTERS.key().regex("abcd\\C*"))
            .filter(FILTERS.key().regex("\\.f\\Ch\\C*"))
            .filter(FILTERS.key().regex("\\C\\C\\C\\C\\C*"));

    Assert.assertEquals(expected.toProto(), adaptedFilter.toProto());
  }

  @Test
  public void singleByteKey() throws IOException {
    Pair<Byte, String>[] testData =
        new Pair[] {
          Pair.newPair((byte) -128, "\\x80"),
          Pair.newPair((byte) -127, "\\x81"),
          Pair.newPair((byte) -42, "\\xD6"),
          Pair.newPair((byte) -2, "\\xFE"),
          Pair.newPair((byte) -1, "\\xFF"),
          Pair.newPair((byte) 0, "\\x00"),
          Pair.newPair((byte) 1, "\\\001"),
          Pair.newPair((byte) 15, "\\\017"),
          Pair.newPair((byte) 42, "\\*"),
          Pair.newPair((byte) 50, "2"),
          Pair.newPair((byte) 70, "F"),
          Pair.newPair((byte) 94, "\\^"),
          Pair.newPair((byte) 95, "_"),
          Pair.newPair((byte) 100, "d"),
          Pair.newPair((byte) 126, "\\~"),
          Pair.newPair((byte) 127, "\\\177")
        };

    byte[] yes = new byte[] {0};
    byte[] no = new byte[] {1};
    Filters.Filter expectedNo = FILTERS.key().regex("\\C\\C*");

    for (Pair<Byte, String> pair : testData) {
      byte[] key = new byte[] {pair.getFirst()};

      FuzzyRowFilter fuzzyYes = new FuzzyRowFilter(ImmutableList.of(Pair.newPair(key, yes)));
      Filters.Filter filterYes = FilterAdapter.buildAdapter().adaptFilter(null, fuzzyYes).get();
      Filters.Filter expectedYes = FILTERS.key().regex(pair.getSecond() + "\\C*");
      Assert.assertEquals(expectedYes.toProto(), filterYes.toProto());

      FuzzyRowFilter fuzzyNo = new FuzzyRowFilter(ImmutableList.of(Pair.newPair(key, no)));
      Filters.Filter filterNo = FilterAdapter.buildAdapter().adaptFilter(null, fuzzyNo).get();
      Assert.assertEquals(expectedNo.toProto(), filterNo.toProto());
    }
  }

  @Test
  public void keysAreProducedFromIntegers() throws IOException {
    Pair<Integer, String>[] testData =
        new Pair[] {
          Pair.newPair(Integer.MIN_VALUE, "\\x80\\x00\\x00\\x00"),
          Pair.newPair(Integer.MIN_VALUE + 1, "\\x80\\x00\\x00\\\001"),
          Pair.newPair(-100500, "\\xFF\\xFEwl"),
          Pair.newPair(-128, "\\xFF\\xFF\\xFF\\x80"),
          Pair.newPair(-127, "\\xFF\\xFF\\xFF\\x81"),
          Pair.newPair(-2, "\\xFF\\xFF\\xFF\\xFE"),
          Pair.newPair(-1, "\\xFF\\xFF\\xFF\\xFF"),
          Pair.newPair(0, "\\x00\\x00\\x00\\x00"),
          Pair.newPair(1, "\\x00\\x00\\x00\\\001"),
          Pair.newPair(2, "\\x00\\x00\\x00\\\002"),
          Pair.newPair(70, "\\x00\\x00\\x00F"),
          Pair.newPair(100, "\\x00\\x00\\x00d"),
          Pair.newPair(126, "\\x00\\x00\\x00\\~"),
          Pair.newPair(127, "\\x00\\x00\\x00\\\177"),
          Pair.newPair(128, "\\x00\\x00\\x00\\x80"),
          Pair.newPair(129, "\\x00\\x00\\x00\\x81"),
          Pair.newPair(7000000, "\\x00j\\xCF\\xC0"),
          Pair.newPair(1131376492, "Cool"),
          Pair.newPair(Integer.MAX_VALUE - 1, "\\\177\\xFF\\xFF\\xFE"),
          Pair.newPair(Integer.MAX_VALUE, "\\\177\\xFF\\xFF\\xFF"),
        };

    for (Pair<Integer, String> pair : testData) {
      byte[] key = createKey(pair.getFirst());

      FuzzyRowFilter fuzzy = new FuzzyRowFilter(ImmutableList.of(Pair.newPair(key, new byte[4])));
      Filters.Filter filter = FilterAdapter.buildAdapter().adaptFilter(null, fuzzy).get();
      Filters.Filter expected = FILTERS.key().regex(pair.getSecond() + "\\C*");
      Assert.assertEquals(expected.toProto(), filter.toProto());
    }
  }

  private static byte[] createKey(int... values) {
    byte[] bytes = new byte[4 * values.length];
    for (int i = 0; i < values.length; i++) {
      System.arraycopy(Bytes.toBytes(values[i]), 0, bytes, 4 * i, 4);
    }
    return bytes;
  }
}
