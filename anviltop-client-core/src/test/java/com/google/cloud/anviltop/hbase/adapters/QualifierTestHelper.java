/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase.adapters;


import org.apache.hadoop.hbase.KeyValue;

import java.nio.ByteBuffer;

/**
 * Helper methods for creating cell qualifiers.
 */
public class QualifierTestHelper {
  public final byte[] HBASE_FAMILY_SEPARATOR = KeyValue.COLUMN_FAMILY_DELIM_ARRAY;

  public byte[] makeFullQualifier(byte[] family, byte[] cellQualifier) {
    byte[] result = new byte[family.length + HBASE_FAMILY_SEPARATOR.length + cellQualifier.length];
    ByteBuffer wrapper = ByteBuffer.wrap(result);
    wrapper.put(family);
    wrapper.put(HBASE_FAMILY_SEPARATOR);
    wrapper.put(cellQualifier);
    return result;
  }
}
