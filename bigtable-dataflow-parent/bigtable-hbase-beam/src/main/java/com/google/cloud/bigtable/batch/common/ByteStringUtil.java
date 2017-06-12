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
package com.google.cloud.bigtable.batch.common;

import org.apache.beam.sdk.io.range.ByteKey;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.util.ByteStringer;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;

/**
 * Converts between shaded and unshaded versions of ByteStrings
 * @author sduskis
 *
 */
public class ByteStringUtil {

  public static ByteKey toByteKey(ByteString byteString) {
    return ByteKey.copyFrom(getBytes(byteString));
  }

  public static com.google.protobuf.ByteString toUnshaded(ByteString byteString) {
    byte[] bytes = getBytes(byteString);
    try {
      return org.apache.hadoop.hbase.util.ByteStringer.wrap(bytes);
    } catch (Throwable e) {
      // There are some nasty Errors that can be thrown here.
      return com.google.protobuf.ByteString.copyFrom(bytes);
    }
  }

  private static byte[] getBytes(ByteString key) {
    try {
      return ByteStringer.extract(key);
    } catch (Exception e) {
      return key.toByteArray();
    }
  }

}
