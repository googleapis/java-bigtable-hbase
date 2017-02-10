/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link BigtableExtendedScan}.
 */
@RunWith(JUnit4.class)
public class BigtableExtendedScanTest {
  @Test
  public void test_simple() throws IOException {
    byte[] key = "test".getBytes();
    byte[] nextKey = BigtableExtendedScan.calculateTheClosestNextRowKeyForPrefix(key);
    Assert.assertEquals((byte) (key[key.length - 1] + 1), nextKey[key.length - 1]);
  }

  @Test
  public void test_unsignedRange() throws IOException {
    byte[] key = new byte[] { 0x7f };
    byte[] nextKey = BigtableExtendedScan.calculateTheClosestNextRowKeyForPrefix(key);
    Assert.assertEquals((byte) 0x80, nextKey[0]);
  }

  @Test
  public void test_trailing0xFF() throws IOException {
    byte trailer = (byte)0xFF;
    byte[] key = new byte[] { 0x01, trailer, trailer, trailer };
    byte[] nextKey = BigtableExtendedScan.calculateTheClosestNextRowKeyForPrefix(key);
    Assert.assertEquals((byte) 0x2, nextKey[0]);
  }
}
