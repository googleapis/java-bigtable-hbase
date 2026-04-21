/*
 * Copyright 2026 Google LLC
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
package org.apache.hadoop.hbase.util;

import com.google.protobuf.ByteString;

/**
 * Hack/Workaround: This class overrides the one in hbase-common to resolve
 * NoClassDefFoundError: com/google/protobuf/LiteralByteString.
 * 
 * HBase 2.x ByteStringer tries to use reflection to access package-private
 * LiteralByteString in Protobuf 2.x/3.x. Since this project forces Protobuf 4.x
 * (required by Beam 2.72.0), that internal class is missing.
 * 
 * This replacement uses public APIs available in Protobuf 4.x.
 */
public final class ByteStringer {
  private ByteStringer() {}

  public static ByteString wrap(final byte[] array) {
    return ByteString.copyFrom(array);
  }

  public static ByteString wrap(final byte[] array, final int offset, final int length) {
    return ByteString.copyFrom(array, offset, length);
  }
}
