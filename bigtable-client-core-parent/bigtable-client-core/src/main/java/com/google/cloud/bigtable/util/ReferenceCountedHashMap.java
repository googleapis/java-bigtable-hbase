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

import com.google.api.core.InternalApi;
import com.google.common.base.MoreObjects;
import java.util.HashMap;
import java.util.Map;

/**
 * Allows for a HashMap to reference-count its contents.
 *
 * <p>Specifically we have subclassed only the methods related to placement and removal. Though
 * there is more functionality which could be used from Java8, we have chosen to not subclass them
 * as we are not expecting their use as of now. Other items have not been subclassed as they do not
 * affect reference counts (e.g.: get).
 */
@InternalApi
public class ReferenceCountedHashMap<K, V> extends HashMap<K, V> {

  // As removal operates via object reference, we must keep this map as Object as
  //  opposed to K typed.
  private final Map<Object, Integer> counterMap;

  private final Callable<V> cleanerCallback;

  /** Constructor for ReferenceCountedHashMap. */
  public ReferenceCountedHashMap() {
    super();
    this.cleanerCallback = null;
    counterMap = new HashMap<>();
  }

  /** Constructor for ReferenceCountedHashMap wth a destructor callable. */
  public ReferenceCountedHashMap(Callable<V> cleanerCallable) {
    super();
    this.cleanerCallback = cleanerCallable;
    counterMap = new HashMap<>();
  }

  /**
   * Put inserts a key-value pair into the map. If the pair is new, we instantiate its reference
   * count and place the pair into the map; otherwise we just increment it (without placement).
   *
   * @param key The key component of the key-value pair.
   * @param value The pair component of the key-value pair.
   * @return The value inserted.
   */
  public synchronized V put(K key, V value) {
    // TODO: change to following when we reach Java 8 (cleaner + faster impl)
    // this.counterMap.merge(objectReference, 1, Integer::sum);
    Integer currentCount = this.updateReference(key, 1);
    if (currentCount == 1) {
      return super.put(key, value);
    }
    return super.get(key);
  }

  /**
   * Remove removes a reference to the key within the context of the reference count. If no
   * references are left, it also removes the pair from the underlying HashMap.
   *
   * @param key The key for which the reference should be removed.
   * @return The value removed, or null if it does not exist.
   */
  public synchronized V remove(Object key) {
    if (!super.containsKey(key)) {
      return null;
    }
    Integer currentCount = this.updateReference(key, -1);
    if (currentCount <= 0) {
      V value = super.remove(key);
      if (cleanerCallback != null) {
        cleanerCallback.call(value);
      }
      return value;
    } else {
      return super.get(key);
    }
  }
  /** Remove all items from hash map and all references from reference counter. */
  public void clear() {
    counterMap.clear();
    super.clear();
  }

  public Object clone() {
    throw new UnsupportedOperationException("Reference-counted objects should not be cloned.");
  }

  public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException("Uncommon method for this use-case.");
  }

  /**
   * Internal helper method to increment/decrement the reference count for a specific key.
   * Side-effect: Deletes item from counterMap if 0.
   *
   * @param key The key reference to be updated.
   * @param updateCount The value to update it to.
   * @return The final count of the reference post update.
   */
  private Integer updateReference(Object key, Integer updateCount) {
    // Not making this synchronized as the methods calling this are expected to be synchronized.
    //  No reason to double lock (note: compiler might eliminate, but unsure)
    Integer currentCount = updateCount + MoreObjects.firstNonNull(counterMap.get(key), 0);
    if (currentCount <= 0) {
      counterMap.remove(key);
    } else {
      counterMap.put(key, currentCount);
    }
    return currentCount;
  }

  /**
   * Interface for destructor code.
   *
   * @param <I> The type of the value to destroy
   */
  public interface Callable<I> {
    void call(I input);
  }
}
