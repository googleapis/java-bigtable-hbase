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

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestRowKeyWrapper {

  @Test
  public void test() {
    RowKeyWrapper rowKeyWrapper = new RowKeyWrapper(ByteString.copyFromUtf8("test-key"));
    assertEquals("test-key", rowKeyWrapper.getKey().toStringUtf8());
  }

  @Test
  public void testCompareRowKeyWrappers() {
    assertEquals(0, new RowKeyWrapper(null).compareTo(new RowKeyWrapper(null)));

    RowKeyWrapper rowKey = new RowKeyWrapper(ByteString.copyFromUtf8("a"));
    assertEquals(-1, rowKey.compareTo(new RowKeyWrapper(null)));
    assertEquals(0, rowKey.compareTo(new RowKeyWrapper(ByteString.copyFromUtf8("a"))));
    assertEquals(1, new RowKeyWrapper(null).compareTo(rowKey));

    RowKeyWrapper anotherRowKey = new RowKeyWrapper(ByteString.copyFromUtf8("b"));
    assertEquals("a".compareTo("b"), rowKey.compareTo(anotherRowKey));
    assertEquals("b".compareTo("a"), anotherRowKey.compareTo(rowKey));
  }
}
