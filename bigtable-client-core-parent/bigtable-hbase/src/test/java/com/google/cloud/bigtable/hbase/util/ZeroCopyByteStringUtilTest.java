/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.util;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class ZeroCopyByteStringUtilTest {

  private static final Random random = new Random(3141592653589L);

  @Test
  public void test_wrap() {
    List<byte[]> values = Arrays.asList(new byte[0], new byte[1], new byte[128], new byte[1024]);
    for (byte[] bytes : values) {
      random.nextBytes(bytes);
      ByteString byteString = ZeroCopyByteStringUtil.wrap(bytes);
      Assert.assertArrayEquals(bytes, byteString.toByteArray());
    }
  }

  @Test
  public void test_get() {
    List<ByteString> values =
        Arrays.asList(
            ByteString.EMPTY,
            ByteString.copyFromUtf8("x"),
            ByteString.copyFromUtf8("some text"),
            ByteString.copyFrom(new byte[128]),
            ByteString.copyFrom(new byte[1024]));

    ByteString rope = null;
    for (ByteString value : values) {
      Assert.assertArrayEquals(value.toByteArray(), ZeroCopyByteStringUtil.get(value));
      rope = rope == null ? value : rope.concat(value);
    }

    Assert.assertArrayEquals(rope.toByteArray(), ZeroCopyByteStringUtil.get(rope));
  }
}
