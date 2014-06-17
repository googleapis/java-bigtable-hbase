package com.google.cloud.anviltop.hbase.adapters;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Helper methods for creating cell qualifiers.
 */
public class QualifierTestHelper {
  public final byte[] HBASE_FAMILY_SEPARATOR = StandardCharsets.UTF_8.encode(":").array();

  public byte[] makeFullQualifier(byte[] family, byte[] cellQualifier) {
    byte[] result = new byte[family.length + HBASE_FAMILY_SEPARATOR.length + cellQualifier.length];
    ByteBuffer wrapper = ByteBuffer.wrap(result);
    wrapper.put(family);
    wrapper.put(HBASE_FAMILY_SEPARATOR);
    wrapper.put(cellQualifier);
    return result;
  }
}
