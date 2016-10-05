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
import com.google.protobuf.BigtableZeroCopyByteStringUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wrapper around {@link com.google.protobuf.BigtableZeroCopyByteStringUtil} for cases where it's
 * not available.
 *
 * @author sduskis
 * @version $Id: $Id
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
      BigtableZeroCopyByteStringUtil.wrap(new byte [0]);
    } catch (IllegalAccessError iae) {
      USE_ZEROCOPYBYTESTRING = false;
      LOG.debug("Failed to classload BigtableZeroCopyByteString: " + iae.toString());
    }
  }

  private ByteStringer() {
    super();
  }

  /**
   * Wraps a byte array in a {@link com.google.protobuf.ByteString} without copying it.
   *
   * @param array an array of byte.
   * @return a {@link com.google.protobuf.ByteString} object.
   */
  public static ByteString wrap(final byte[] array) {
    return USE_ZEROCOPYBYTESTRING
        ? BigtableZeroCopyByteStringUtil.wrap(array)
        : ByteString.copyFrom(array);
  }

  /**
   * extract.
   *
   * @param buf a {@link com.google.protobuf.ByteString} object.
   * @return an array of byte.
   */
  public static byte[] extract(ByteString buf) {
    return USE_ZEROCOPYBYTESTRING
        ? BigtableZeroCopyByteStringUtil.zeroCopyGetBytes(buf)
        : buf.toByteArray();
  }
}
