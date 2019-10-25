/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

import com.google.cloud.bigtable.util.ReferenceCountedHashMap.Callable;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/** Created by j-clap on 10/17/19. */
public class ReferenceCountedHashMapTest {

  private final String[] KEYS = {"first", "second"};
  private final Integer[] VALUES = {1, 2};

  @Test
  public void test_insertionWorks() {
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    testMap.put(KEYS[0], VALUES[0]);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    testMap.put(KEYS[1], VALUES[1]);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    Assert.assertEquals(testMap.get(KEYS[1]), VALUES[1]);
    Assert.assertEquals(testMap.size(), 2);
  }

  @Test
  public void test_multipleInsertionDoesNotUpdate() {
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    testMap.put(KEYS[0], VALUES[0]);
    testMap.put(KEYS[0], null);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    testMap.put(KEYS[0], -1);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    Assert.assertEquals(testMap.size(), 1);
  }

  @Test
  public void test_emptyRemoveReturnsNull() {
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    Assert.assertNull(testMap.remove(KEYS[0]));
  }

  @Test
  public void test_emptyDoesntCallCallback() {
    final int[] accessor = {999};
    Map<String, Integer> testMap =
        new ReferenceCountedHashMap<>(
            new Callable<Integer>() {
              @Override
              public void call(Integer input) {
                accessor[0] = input;
              }
            });
    Assert.assertNull(testMap.remove(KEYS[0]));
    Assert.assertEquals(accessor[0], 999);
  }

  @Test
  public void test_removesOnlyReferencedItem() {
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    testMap.put(KEYS[0], VALUES[0]);
    testMap.put(KEYS[1], VALUES[1]);
    Assert.assertEquals(testMap.size(), 2);
    Assert.assertEquals(testMap.remove(KEYS[0]), VALUES[0]);
    Assert.assertNull(testMap.get(KEYS[0]));
    Assert.assertEquals(testMap.get(KEYS[1]), VALUES[1]);
    Assert.assertEquals(testMap.size(), 1);
  }

  @Test
  public void test_removeCallsCallback() {
    final int[] accessor = {999};
    Map<String, Integer> testMap =
        new ReferenceCountedHashMap<>(
            new Callable<Integer>() {
              @Override
              public void call(Integer input) {
                accessor[0] = input;
              }
            });
    testMap.put(KEYS[0], VALUES[0]);
    testMap.put(KEYS[1], VALUES[1]);
    Assert.assertEquals(testMap.size(), 2);
    Assert.assertEquals(testMap.remove(KEYS[0]), VALUES[0]);
    Assert.assertNull(testMap.get(KEYS[0]));
    Assert.assertEquals(testMap.get(KEYS[1]), VALUES[1]);
    Assert.assertEquals(testMap.size(), 1);
    Assert.assertEquals(accessor[0], 1);
  }

  @Test
  public void test_removeOnlyDeletesWhenRefCountIs0() {
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    testMap.put(KEYS[0], VALUES[0]);
    testMap.put(KEYS[0], VALUES[0]);
    testMap.put(KEYS[0], VALUES[0]);
    Assert.assertEquals(testMap.remove(KEYS[0]), VALUES[0]);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    Assert.assertEquals(testMap.remove(KEYS[0]), VALUES[0]);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    Assert.assertEquals(testMap.remove(KEYS[0]), VALUES[0]);
    Assert.assertNull(testMap.get(KEYS[0]));
  }

  @Test
  public void test_clear() {
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    testMap.clear();
    Assert.assertEquals(testMap.size(), 0);
    testMap.put(KEYS[0], VALUES[0]);
    testMap.put(KEYS[0], VALUES[0]);
    testMap.put(KEYS[1], VALUES[1]);
    testMap.put(KEYS[1], VALUES[1]);
    Assert.assertEquals(testMap.size(), 2);
    testMap.clear();
    Assert.assertEquals(testMap.size(), 0);
    Assert.assertNull(testMap.get(KEYS[0]));
    Assert.assertNull(testMap.get(KEYS[1]));
  }
}
