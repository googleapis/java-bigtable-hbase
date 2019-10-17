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

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/** Created by j-clap on 10/17/19. */
public class ReferenceCountedHashMapTest {

  private final String[] KEYS = {"first", "second"};
  private final Integer[] VALUES = {1, 2};

  @Test
  public void test_insertion_works() {
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    testMap.put(KEYS[0], VALUES[0]);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    testMap.put(KEYS[1], VALUES[1]);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    Assert.assertEquals(testMap.get(KEYS[1]), VALUES[1]);
    Assert.assertEquals(testMap.size(), 2);
  }

  @Test
  public void test_multiple_insertion_does_not_update() {
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    testMap.put(KEYS[0], VALUES[0]);
    testMap.put(KEYS[0], null);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    testMap.put(KEYS[0], -1);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    Assert.assertEquals(testMap.size(), 1);
  }

  @Test
  public void test_empty_remove_returns_null() {
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    Assert.assertNull(testMap.remove(KEYS[0]));
  }

  @Test
  public void test_removes_only_referenced_item() {
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
  public void test_remove_only_deletes_when_ref_count_is_0() {
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

  @Test
  public void test_put_all() {
    Map<String, Integer> insertionMap = new HashMap<>();
    Map<String, Integer> testMap = new ReferenceCountedHashMap<>();
    insertionMap.put(KEYS[0], VALUES[0]);
    insertionMap.put(KEYS[1], VALUES[1]);
    testMap.putAll(insertionMap);
    Assert.assertEquals(testMap.get(KEYS[0]), VALUES[0]);
    Assert.assertEquals(testMap.get(KEYS[1]), VALUES[1]);
    Assert.assertEquals(testMap.size(), 2);
  }
}
