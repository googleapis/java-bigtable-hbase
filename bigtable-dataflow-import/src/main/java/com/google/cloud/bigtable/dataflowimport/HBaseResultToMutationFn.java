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
package com.google.cloud.bigtable.dataflowimport;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} function that converts a {@link Result} in the pipeline input to a
 * {@link Mutation} for output.
 */
class HBaseResultToMutationFn
    extends DoFn<KV<ImmutableBytesWritable, Result>, Mutation> {
  private static Logger logger = LoggerFactory.getLogger(HBaseImportIO.class);

  private static final long serialVersionUID = 1L;
  private static final Cell[] NULL_REPLACEMENT = new Cell[0];

  private transient boolean isEmptyRowWarned;

  @VisibleForTesting
  static void setLogger(Logger log) {
    logger = log;
  }

  @Override
  public void processElement(ProcessContext context) throws Exception {
    KV<ImmutableBytesWritable, Result> kv = context.element();
    Cell[] cells = checkEmptyRow(kv);
    if (cells.length == 0) {
      return;
    }
    // TODO: Handle DeleteMarkers properly.
    Put put = new Put(kv.getKey().get());
    for (Cell cell : cells) {
      put.add(cell);
    }
    context.output(put);
  }

  // Warns about empty row on first occurrence only and replaces a null array with 0-length one.
  private Cell[] checkEmptyRow(KV<ImmutableBytesWritable, Result> kv) {
    Cell[] cells = kv.getValue().rawCells();
    if (cells == null) {
      cells = NULL_REPLACEMENT;
    }
    if (!isEmptyRowWarned && cells.length == 0) {
      logger.warn("Encountered empty row. Was input file serialized by HBase 0.94?");
      isEmptyRowWarned = true;
    }
    return cells;
  }
}
