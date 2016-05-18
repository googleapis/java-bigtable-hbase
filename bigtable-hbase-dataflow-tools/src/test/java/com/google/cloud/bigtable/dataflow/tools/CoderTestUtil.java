/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.dataflow.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Simple tool to help with testing {@link BigtableConverter}s.
 */
public class CoderTestUtil {

  public static <T> T encodeAndDecode(BigtableConverter<T> converter, T original)
      throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(encode(converter, original));
    return converter.decode(bais);
  }

  public static <T> byte[] encode(BigtableConverter<T> coder, T original) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    coder.encode(original, bos);
    return bos.toByteArray();
  }
}
