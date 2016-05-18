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
package com.google.cloud.bigtable.dataflow.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.client.Result;

import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.hbase.adapters.Adapters;

/**
 * A {@link BigtableConverter} that serializes and deserializes the {@link Result}.
 */
public class HBaseResultConverter implements BigtableConverter<Result> {

  private static final long serialVersionUID = -4975428837770254686L;

  private static final HBaseResultConverter INSTANCE = new HBaseResultConverter();

  public static HBaseResultConverter getInstance() {
    return INSTANCE;
  }

  @Override
  public Result decode(InputStream inputStream)
      throws IOException {
    return Adapters.ROW_ADAPTER.adaptResponse(Row.parseDelimitedFrom(inputStream));
  }

  @Override
  public void encode(Result value, OutputStream outputStream)
      throws IOException {
    Adapters.ROW_ADAPTER.adaptToRow(value).writeDelimitedTo(outputStream);
  }
}
