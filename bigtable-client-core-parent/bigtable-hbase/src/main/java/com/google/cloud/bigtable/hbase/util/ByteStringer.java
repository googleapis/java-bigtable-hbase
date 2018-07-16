/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.ByteString;

/**
 * Wrapper around {@link ZeroCopyByteStringUtil} for cases where it's not available.
 */
public class ByteStringer {
  private static final Log LOG = LogFactory.getLog(ByteStringer.class);

  /**
   * Flag set at class loading time.
   */
  private static boolean USE_ZEROCOPYBYTESTRING = true;

  // Can I classload BigtableZeroCopyByteStringUtil without IllegalAccessError?
  // If we can, use it passing ByteStrings to pb else use native ByteString though more costly
  // because it makes a copy of the passed in array.
  static {
    try {
      ZeroCopyByteStringUtil.wrap(new byte [0]);
    } catch (IllegalAccessError iae) {
      USE_ZEROCOPYBYTESTRING = false;
      LOG.debug("Failed to classload BigtableZeroCopyByteString: " + iae.toString());
    }
  }

  private ByteStringer() {
    super();
  }

  /**
   * Wraps a byte array in a {@link ByteString} without copying it.
   */
  public static ByteString wrap(final byte[] array) {
    return USE_ZEROCOPYBYTESTRING? ZeroCopyByteStringUtil.wrap(array): ByteString.copyFrom(array);
  }

  /**
   * Wraps a byte array in a {@link ByteString} without copying it.
   */
  public static ByteString wrap(final byte[] array, int offset, int length) {
    if (USE_ZEROCOPYBYTESTRING && offset == 0 && length == array.length) {
      return ZeroCopyByteStringUtil.wrap(array);
    }
    else {
      return ByteString.copyFrom(array, offset, length);
    }
  }

  public static byte[] extract(ByteString buf) {
    return USE_ZEROCOPYBYTESTRING ? ZeroCopyByteStringUtil.get(buf) : buf
        .toByteArray();
  }
}
