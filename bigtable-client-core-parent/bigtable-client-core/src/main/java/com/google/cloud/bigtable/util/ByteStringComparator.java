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
package com.google.cloud.bigtable.util;

import java.util.Comparator;

import com.google.protobuf.ByteString;

/**
 * Compares {@link ByteString}s.
 */
public final class ByteStringComparator implements Comparator<ByteString>{

  public static final ByteStringComparator INSTANCE = new ByteStringComparator();

  @Override
  public int compare(ByteString key1, ByteString key2) {
    int size1 = key1.size();
    int size2 = key2.size();
    int size = Math.min(size1, size2);

    for (int i = 0; i < size; i++) {
      // compare bytes as unsigned
      int byte1 = key1.byteAt(i) & 0xff;
      int byte2 = key2.byteAt(i) & 0xff;

      int comparison = Integer.compare(byte1, byte2);
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(size1, size2);
  }
}
