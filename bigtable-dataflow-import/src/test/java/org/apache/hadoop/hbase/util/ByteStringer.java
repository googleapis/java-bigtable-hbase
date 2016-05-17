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
package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.cloud.bigtable.dataflowimport.testing.SequenceFileIoUtils;
import com.google.protobuf.BigtableZeroCopyByteStringUtil;
import com.google.protobuf.ByteString;

/**
 * Hack to make {@link SequenceFileIoUtils#createFileWithData(java.io.File, java.util.Collection)}
 * work with the newer version of protobuf required by bigtable.
 */
@InterfaceAudience.Private
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
   * Wraps a byte array in a {@link ByteString} without copying it.
   */
  public static ByteString wrap(final byte[] array) {
    return USE_ZEROCOPYBYTESTRING? BigtableZeroCopyByteStringUtil.wrap(array): ByteString.copyFrom(array);
  }

  /**
   * Wraps a subset of a byte array in a {@link ByteString} without copying it.
   */
  public static ByteString wrap(final byte[] array, int offset, int length) {
    return USE_ZEROCOPYBYTESTRING? BigtableZeroCopyByteStringUtil.wrap(array, offset, length):
      ByteString.copyFrom(array, offset, length);
  }
}
