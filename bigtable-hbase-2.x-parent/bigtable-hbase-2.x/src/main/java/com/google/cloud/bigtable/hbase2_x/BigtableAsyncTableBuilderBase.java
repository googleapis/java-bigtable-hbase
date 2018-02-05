/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase2_x;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTableBuilder;
import org.apache.hadoop.hbase.client.RawAsyncTable;

/**
 * Abstract class for implementing Bigtable version of {@link AsyncTable} and {@link RawAsyncTable}
 * 
 * @author spollapally
 */
public abstract class BigtableAsyncTableBuilderBase<T extends AsyncTable>
    implements AsyncTableBuilder<T> {

  public BigtableAsyncTableBuilderBase() {
  }
  
  @Override
  public AsyncTableBuilder<T> setMaxAttempts(int arg0) {
    throw new UnsupportedOperationException("setMaxAttempts"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setOperationTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setOperationTimeout"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setReadRpcTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setReadRpcTimeout"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setRetryPause(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setRetryPause"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setRpcTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setRpcTimeout"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setScanTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setScanTimeout"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setStartLogErrorsCnt(int arg0) {
    throw new UnsupportedOperationException("setStartLogErrorsCnt"); // TODO
  }

  @Override
  public AsyncTableBuilder<T> setWriteRpcTimeout(long arg0, TimeUnit arg1) {
    throw new UnsupportedOperationException("setWriteRpcTimeout"); // TODO
  }
}
