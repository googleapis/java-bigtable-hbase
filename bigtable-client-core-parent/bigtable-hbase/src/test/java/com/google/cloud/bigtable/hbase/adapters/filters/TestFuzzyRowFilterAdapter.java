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
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    Map<Byte, String> testData = new HashMap<>();
    testData.put((byte) -128, "\\x80");
    testData.put((byte) -127, "\\x81");
    testData.put((byte) -42, "\\xD6");
    testData.put((byte) -2, "\\xFE");
    testData.put((byte) -1, "\\xFF");
    testData.put((byte) 0, "\\x00");
    testData.put((byte) 1, "\\\001");
    testData.put((byte) 15, "\\\017");
    testData.put((byte) 42, "\\*");
    testData.put((byte) 50, "2");
    testData.put((byte) 70, "F");
    testData.put((byte) 94, "\\^");
    testData.put((byte) 95, "_");
    testData.put((byte) 100, "d");
    testData.put((byte) 126, "\\~");
    testData.put((byte) 127, "\\\177");

    byte[] yes = new byte[] {0};
    byte[] no = new byte[] {1};
    Filters.Filter expectedNo = FILTERS.key().regex("\\C\\C*");

    for (Byte byteKey : testData.keySet()) {
      byte[] key = new byte[] {byteKey};

      FuzzyRowFilter fuzzyYes = new FuzzyRowFilter(ImmutableList.of(Pair.newPair(key, yes)));
      Filters.Filter filterYes = FilterAdapter.buildAdapter().adaptFilter(null, fuzzyYes).get();
      Filters.Filter expectedYes = FILTERS.key().regex(testData.get(byteKey) + "\\C*");
      Assert.assertEquals(expectedYes.toProto(), filterYes.toProto());

      FuzzyRowFilter fuzzyNo = new FuzzyRowFilter(ImmutableList.of(Pair.newPair(key, no)));
      Filters.Filter filterNo = FilterAdapter.buildAdapter().adaptFilter(null, fuzzyNo).get();
      Assert.assertEquals(expectedNo.toProto(), filterNo.toProto());
    }
  }

  @Test
  public void keysAreProducedFromIntegers() throws IOException {
    Map<Integer, String> testData = new HashMap<>();
    testData.put(Integer.MIN_VALUE, "\\x80\\x00\\x00\\x00");
    testData.put(Integer.MIN_VALUE + 1, "\\x80\\x00\\x00\\\001");
    testData.put(-100500, "\\xFF\\xFEwl");
    testData.put(-128, "\\xFF\\xFF\\xFF\\x80");
    testData.put(-127, "\\xFF\\xFF\\xFF\\x81");
    testData.put(-2, "\\xFF\\xFF\\xFF\\xFE");
    testData.put(-1, "\\xFF\\xFF\\xFF\\xFF");
    testData.put(0, "\\x00\\x00\\x00\\x00");
    testData.put(1, "\\x00\\x00\\x00\\\001");
    testData.put(2, "\\x00\\x00\\x00\\\002");
    testData.put(70, "\\x00\\x00\\x00F");
    testData.put(100, "\\x00\\x00\\x00d");
    testData.put(126, "\\x00\\x00\\x00\\~");
    testData.put(127, "\\x00\\x00\\x00\\\177");
    testData.put(128, "\\x00\\x00\\x00\\x80");
    testData.put(129, "\\x00\\x00\\x00\\x81");
    testData.put(7000000, "\\x00j\\xCF\\xC0");
    testData.put(1131376492, "Cool");
    testData.put(Integer.MAX_VALUE - 1, "\\\177\\xFF\\xFF\\xFE");
    testData.put(Integer.MAX_VALUE, "\\\177\\xFF\\xFF\\xFF");

    for (Integer intKey : testData.keySet()) {
      byte[] key = createKey(intKey);
      FuzzyRowFilter fuzzy = new FuzzyRowFilter(ImmutableList.of(Pair.newPair(key, new byte[4])));
      Filters.Filter filter = FilterAdapter.buildAdapter().adaptFilter(null, fuzzy).get();
      Filters.Filter expected = FILTERS.key().regex(testData.get(intKey) + "\\C*");
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
