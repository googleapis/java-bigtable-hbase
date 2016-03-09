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

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link Coder} that serializes and deserializes the {@link Result} array using
 * {@link ProtobufUtil}.
 */
public class HBaseResultArrayCoder extends AtomicCoder<Result[]>{

  private static final HBaseResultArrayCoder INSTANCE = new HBaseResultArrayCoder();

  public static HBaseResultArrayCoder getInstance() {
    return INSTANCE;
  }

  private static final long serialVersionUID = -4975428837770254686L;

  @Override
  public Result[] decode(InputStream inputStream, Coder.Context context)
      throws CoderException, IOException {
    ScanResponse response = ScanResponse.parseDelimitedFrom(inputStream);
    Result[] results = new Result[response.getResultsCount()];
    for (int i = 0; i < results.length; i++) {
      results[i] = ProtobufUtil.toResult(response.getResults(i));
    }
    return results;
  }

  @Override
  public void encode(Result[] results, OutputStream outputStream, Coder.Context context)
      throws CoderException, IOException {
    ScanResponse.Builder scanResponseBuilder = ScanResponse.newBuilder();
    for (Result result : results) {
      scanResponseBuilder.addResults(ProtobufUtil.toResult(result));
    }
    scanResponseBuilder.build().writeDelimitedTo(outputStream);
  }

}
