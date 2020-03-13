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
package com.google.cloud.bigtable.util;

import com.google.protobuf.ByteString;
import java.util.Comparator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ByteStringComparatorTest {

  Comparator<ByteString> underTest = ByteStringComparator.INSTANCE;

  @Test
  public void testSimple() {
    compare("a", "c");
    compare("aa", "cc");
    compare("aa", "c");
    compare("a", "cc");
    compare("aa", "aac");
  }

  @Test
  public void testUnsigned() {
    compare(ByteString.copyFrom(new byte[] {0x7f}), ByteString.copyFrom(new byte[] {(byte) 0x80}));
  }

  private void compare(String a, String b) {
    compare(ByteString.copyFromUtf8(a), ByteString.copyFromUtf8(b));
  }

  private void compare(ByteString a, ByteString b) {
    Assert.assertTrue(underTest.compare(a, b) < 0);
    Assert.assertTrue(underTest.compare(b, a) > 0);
    Assert.assertTrue(underTest.compare(b, b) == 0);
  }
}
