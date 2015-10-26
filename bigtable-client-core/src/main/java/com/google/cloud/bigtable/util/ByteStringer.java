package com.google.cloud.bigtable.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.BigtableZeroCopyByteString;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wrapper around {@link BigtableZeroCopyByteString} for cases where it's not available.
 */
public class ByteStringer {
  private static final Log LOG = LogFactory.getLog(ByteStringer.class);
  
  /**
   * Flag set at class loading time.
   */
  private static boolean USE_ZEROCOPYBYTESTRING = true;
  
  // Can I classload HBaseZeroCopyByteString without IllegalAccessError?
  // If we can, use it passing ByteStrings to pb else use native ByteString though more costly
  // because it makes a copy of the passed in array.
  static {
    try {
      BigtableZeroCopyByteString.wrap(new byte [0]);
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
    return USE_ZEROCOPYBYTESTRING? BigtableZeroCopyByteString.wrap(array): ByteString.copyFrom(array);
  }
  
  /**
   * Wraps a subset of a byte array in a {@link ByteString} without copying it.
   */
  public static ByteString wrap(final byte[] array, int offset, int length) {
    return USE_ZEROCOPYBYTESTRING? BigtableZeroCopyByteString.wrap(array, offset, length):
      ByteString.copyFrom(array, offset, length);
  }

  /**
   * Attempts to extract the byte[] from the input wiht zeroCopy if possible.
   */
  public static byte[] extractBytes(ByteString value) {
    return USE_ZEROCOPYBYTESTRING ? BigtableZeroCopyByteString.zeroCopyGetBytes(value) :
      value.toByteArray();
  }
}
