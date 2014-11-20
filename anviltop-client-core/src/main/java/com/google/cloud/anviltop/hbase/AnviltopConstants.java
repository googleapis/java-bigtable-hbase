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
package com.google.cloud.anviltop.hbase;


import com.google.protobuf.ByteString;

import java.util.concurrent.TimeUnit;

/**
 * Constants related to Anviltop.
 */
public class AnviltopConstants {

  /**
   * Separator between column family and column name for anviltop, as a single byte.
   */
  public static final byte ANVILTOP_COLUMN_SEPARATOR_BYTE = (byte)':';

  /**
   * The length of the column separator, in bytes.
   */
  public static final int ANVILTOP_COLUMN_SEPARATOR_LENGTH = 1;

  /**
   * Byte string of the column family and column name separator for anviltop.
   */
  public static final ByteString ANVILTOP_COLUMN_SEPARATOR_BYTE_STRING =
      ByteString.copyFrom(new byte[] {ANVILTOP_COLUMN_SEPARATOR_BYTE});

  /**
   * TimeUnit in which HBase clients expects messages to be sent and received.
   */
  public static final TimeUnit HBASE_TIMEUNIT = TimeUnit.MILLISECONDS;

  /**
   * TimeUnit in which Anviltop requires messages to be sent and received.
   */
  public static final TimeUnit ANVILTOP_TIMEUNIT = TimeUnit.MICROSECONDS;
}
