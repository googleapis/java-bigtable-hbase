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

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;

/**
 * Compares {@link ByteString}s.
 */
public final class ByteStringComparator implements Comparator<ByteString> {

  // TODO: Newer versions of ByteStrings have a comparator.  We should use that comparator
  // once the protobuf dependencies are upgraded.
  public static final Comparator<ByteString> INSTANCE = new ByteStringComparator();

  @Override
  public int compare(ByteString key1, ByteString key2) {
    if (key1 == null) {
      return key2 == null ? 0 : 1;
    } else if (key2 == null) {
      return -1;
    }

    if (key1 == key2) {
      return 0;
    }

    int size = Math.min(key1.size(), key2.size());

    for (int i = 0; i < size; i++) {
      int comparison = UnsignedBytes.compare(key1.byteAt(i), key2.byteAt(i));
      if (comparison != 0) {
        return comparison;
      }
    }
    return Integer.compare(key1.size(), key2.size());
  }
}
