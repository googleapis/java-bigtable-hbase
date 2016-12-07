/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.google.protobuf;

import com.google.cloud.bigtable.dataflowimport.ZeroCopyByteStringUtil;

/**
 * Overrides the default HBaseZeroCopyByteString from the hbase implementation so that this class
 * can work with protobuf 3.0.0.
 */
public final class HBaseZeroCopyByteString  {

  // Gotten from AsyncHBase code base with permission.
  /** Private constructor so this class cannot be instantiated. */
  private HBaseZeroCopyByteString() {
    throw new UnsupportedOperationException("Should never be here.");
  }

  /**
   * Wraps a byte array in a {@link ByteString} without copying it.
   * @param array array to be wrapped
   * @return wrapped array
   */
  public static ByteString wrap(final byte[] array) {
    return ZeroCopyByteStringUtil.wrap(array);
  }

  /**
   * Wraps a subset of a byte array in a {@link ByteString} without copying it.
   * @param array array to be wrapped
   * @param offset from
   * @param length length
   * @return wrapped array
   */
  public static ByteString wrap(final byte[] array, int offset, int length) {
    return ByteString.copyFrom(array, offset, length);
  }

  /**
   * Extracts the byte array from the given {@link ByteString} without copy.
   * @param buf A buffer from which to extract the array.
   * @return byte[] representation
   */
  public static byte[] zeroCopyGetBytes(final ByteString buf) {
    return ZeroCopyByteStringUtil.get(buf);
  }
}
