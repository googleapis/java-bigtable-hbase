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
package com.google.cloud.bigtable.dataflow.coders;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * An {@link AtomicCoder} that serializes and deserializes the {@link Result} array.
 */
public class HBaseResultArrayCoder extends AtomicCoder<Result[]>{

  private static final HBaseResultArrayCoder INSTANCE = new HBaseResultArrayCoder();

  public static HBaseResultArrayCoder getInstance() {
    return INSTANCE;
  }

  private static final long serialVersionUID = -4975428837770254686L;

  @Override
  public void encode(Result[] results, OutputStream outputStream) throws CoderException, IOException {
    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
    oos.writeInt(results.length);
    oos.flush();
    for (Result result : results) {
      HBaseResultCoder.getInstance().encode(result, oos);
    }
  }

  @Override
  public Result[] decode(InputStream inputStream) throws CoderException, IOException {
    ObjectInputStream ois = new ObjectInputStream(inputStream);
    int resultCount = ois.readInt();
    Result[] results = new Result[resultCount];
    for (int i = 0; i < resultCount; i++) {
      results[i] = HBaseResultCoder.getInstance().decode(ois);
    }
    return results;
  }
}
