/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.beam.validation;

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import com.google.common.collect.ImmutableList;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Wraps HashTable.TableHash object and delegates the calls to it. This class exposes the minimal
 * interface required from TableHash. This class is required for mocking purposes in unit tests.
 */
@InternalApi
public interface TableHashWrapper extends Serializable {

  int getNumHashFiles();

  ImmutableList<ImmutableBytesWritable> getPartitions();

  ImmutableBytesWritable getStartRow();

  ImmutableBytesWritable getStopRow();

  Scan getScan();

  TableHashReader newReader(Configuration conf, ImmutableBytesWritable startRow);

  interface TableHashReader extends Closeable {
    boolean next() throws IOException;

    ImmutableBytesWritable getCurrentKey();

    ImmutableBytesWritable getCurrentHash();

    void close() throws IOException;
  }
}
