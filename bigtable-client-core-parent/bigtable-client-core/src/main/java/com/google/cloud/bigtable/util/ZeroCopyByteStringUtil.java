package com.google.cloud.bigtable.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.UnsafeByteOperations;
import com.google.protobuf.ByteOutput;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;


/**
 * Helper class to extract byte arrays from {@link com.google.protobuf.ByteString} without copy.
 * <p>
 * Without this protobufs would force us to copy every single byte array out of the objects
 * de-serialized from the wire (which already do one copy, on top of the copies the JVM does to go
 * from kernel buffer to C buffer and from C buffer to JVM buffer).
 *
 * @author sduskis
 * @version $Id: $Id
 */
public final class ZeroCopyByteStringUtil {

  /**
   * Wraps a byte array in a {@link com.google.protobuf.ByteString} without copying it.
   *
   * @param array an array of byte.
   * @return a {@link com.google.protobuf.ByteString} object.
   */
  public static ByteString wrap(final byte[] array) {
    return UnsafeByteOperations.unsafeWrap(ByteBuffer.wrap(array));
  }

  /**
   * Wraps a subset of a byte array in a {@link com.google.protobuf.ByteString} without copying it.
   *
   * @param array an array of byte.
   * @param offset a int.
   * @param length a int.
   * @return a {@link com.google.protobuf.ByteString} object.
   */
  public static ByteString wrap(final byte[] array, int offset, int length) {
    return UnsafeByteOperations.unsafeWrap(ByteBuffer.wrap(array, offset, length));
  }

  /**
   * Extracts the byte array from the given {@link com.google.protobuf.ByteString} without copy.
   *
   * @param buf A buffer from which to extract the array.
   * @return an array of byte.
   */
  public static byte[] get(final ByteString byteString) {
    try {
      ZeroCopyByteOutput byteOutput = new ZeroCopyByteOutput();
      UnsafeByteOperations.unsafeWriteTo(byteString, byteOutput);
      return byteOutput.bytes;
    } catch (IOException e) {
      return byteString.toByteArray();
    }
  }

  private static final class ZeroCopyByteOutput extends ByteOutput {
    private byte[] bytes;

    @Override
    public void writeLazy(byte[] value, int offset, int length) throws IOException {
      if (offset != 0) {
        throw new UnsupportedOperationException();
      }
      bytes = value;
    }

    @Override
    public void write(byte value) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(byte[] value, int offset, int length) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void write(ByteBuffer value) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeLazy(ByteBuffer value) throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
