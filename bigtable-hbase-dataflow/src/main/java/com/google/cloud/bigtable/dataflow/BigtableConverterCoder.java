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
package com.google.cloud.bigtable.dataflow;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.bigtable.repackaged.com.google.cloud.dataflow.tools.BigtableConverter;
import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;

public class BigtableConverterCoder<T> extends AtomicCoder<T> {

  private static final long serialVersionUID = 864682049542548090L;

  private BigtableConverter<T> converter;

  // The default constructor is required for serialization.
  BigtableConverterCoder() {
  }

  public BigtableConverterCoder(BigtableConverter<T> converter) {
    this.converter = converter;
  }

  @Override
  public void encode(T value, OutputStream outStream,
      Coder.Context context)
          throws CoderException, IOException {
    converter.encode(value, outStream);
  }

  @Override
  public T decode(InputStream inStream, Coder.Context context)
      throws CoderException, IOException {
    return converter.decode(inStream);
  }

  public BigtableConverter<T> getConverter() {
    return converter;
  }
}
