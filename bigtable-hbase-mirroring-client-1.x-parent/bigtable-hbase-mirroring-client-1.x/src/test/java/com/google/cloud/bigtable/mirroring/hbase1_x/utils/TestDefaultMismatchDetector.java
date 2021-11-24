/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.mirroring.hbase1_x.verification.DefaultMismatchDetector.LazyBytesHexlifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestDefaultMismatchDetector {
  @Test
  public void testHexlifier() {
    assertThat(new LazyBytesHexlifier(new byte[] {}, 0).toString()).isEqualTo("");
    assertThat(new LazyBytesHexlifier(new byte[] {1}, 0).toString()).isEqualTo("");
    assertThat(new LazyBytesHexlifier(new byte[] {1, 2}, 1).toString()).isEqualTo("01...");
    assertThat(new LazyBytesHexlifier(new byte[] {1, 2, 3}, 2).toString()).isEqualTo("01...03");
    assertThat(
            new LazyBytesHexlifier(new byte[] {(byte) 0x00, (byte) 0x80, (byte) 0xFF}, 2)
                .toString())
        .isEqualTo("00...FF");
    assertThat(
            new LazyBytesHexlifier(
                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 100)
                .toString())
        .isEqualTo("0102030405060708090A0B0C0D0E0F10");
    assertThat(
            new LazyBytesHexlifier(
                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, 5)
                .toString())
        .isEqualTo("010203...0F10");
  }

  @Test
  public void testListHexlifier() {
    Get g1 = new Get(new byte[] {1, 2, 3, 4});
    Get g2 = new Get(new byte[] {(byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4});

    List<Get> list = new ArrayList<>();
    assertThat(LazyBytesHexlifier.listOfHexRows(list, 100).toString()).isEqualTo("[]");

    list.add(g1);
    assertThat(LazyBytesHexlifier.listOfHexRows(list, 100).toString()).isEqualTo("[01020304]");

    list.add(g2);
    assertThat(LazyBytesHexlifier.listOfHexRows(list, 100).toString())
        .isEqualTo("[01020304, F1F2F3F4]");
    assertThat(LazyBytesHexlifier.listOfHexRows(list, 2).toString())
        .isEqualTo("[01...04, F1...F4]");
  }
}
