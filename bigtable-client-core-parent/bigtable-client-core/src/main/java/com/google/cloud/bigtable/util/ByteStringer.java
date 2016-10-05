/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

/**
 * Wrapper around {@link ZeroCopyByteStringUtil}.  This is used for historical purposes.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class ByteStringer {
  /**
   * Wraps a byte array in a {@link com.google.protobuf.ByteString} without copying it.
   *
   * @param array an array of byte.
   * @return a {@link com.google.protobuf.ByteString} object.
   */
  public static ByteString wrap(final byte[] array) {
    return ZeroCopyByteStringUtil.wrap(array);
  }

  /**
   * extract.
   *
   * @param buf a {@link com.google.protobuf.ByteString} object.
   * @return an array of byte.
   */
  public static byte[] extract(ByteString buf) {
    return ZeroCopyByteStringUtil.get(buf);
  }
}
