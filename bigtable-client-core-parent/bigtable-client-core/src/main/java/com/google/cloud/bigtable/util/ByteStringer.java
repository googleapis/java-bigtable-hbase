package com.google.cloud.bigtable.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.BigtableZeroCopyByteStringUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wrapper around {@link com.google.protobuf.BigtableZeroCopyByteStringUtil} for cases where it's not available.
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
    return USE_ZEROCOPYBYTESTRING? BigtableZeroCopyByteStringUtil.wrap(array): ByteString.copyFrom(array);
  }

  /**
   * <p>extract.</p>
   *
   * @param buf a {@link com.google.protobuf.ByteString} object.
   * @return an array of byte.
   */
  public static byte[] extract(ByteString buf) {
    return USE_ZEROCOPYBYTESTRING ? BigtableZeroCopyByteStringUtil.zeroCopyGetBytes(buf) : buf
        .toByteArray();
  }
}
