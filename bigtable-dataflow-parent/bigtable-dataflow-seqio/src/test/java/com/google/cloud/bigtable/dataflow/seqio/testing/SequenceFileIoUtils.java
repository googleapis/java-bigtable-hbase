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
package com.google.cloud.bigtable.dataflow.seqio.testing;

import com.google.cloud.bigtable.dataflow.seqio.HBaseImportIO;
import com.google.cloud.bigtable.dataflow.seqio.HBaseImportOptions;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.SourceTestUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.serializer.WritableSerialization;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Helper methods for creating and reading HBase Sequence Files in tests.
 */
public class SequenceFileIoUtils {

  /**
   * Creates a Sequence File and writes the content of {@code keyValues} to it.
   */
  public static void createFileWithData(File sequenceFile, Collection<? extends Cell> keyValues)
      throws IOException {
    Configuration configuration = new Configuration();
    // Configure serializers for Sequence File writer.
    configuration.set("io.serializations",
        WritableSerialization.class.getName() + "," + ResultSerialization.class.getName());
    // Group cells by rowKey.
    ImmutableListMultimap<byte[], ? extends Cell> records =
        Multimaps.index(keyValues, new Function<Cell, byte[]>() {
          @Override
          public byte[] apply(Cell cell) {
            return CellUtil.cloneRow(cell);
          }
        });
    try (Writer writer = SequenceFile.createWriter(
        configuration,
        Writer.keyClass(ImmutableBytesWritable.class),
        Writer.valueClass(Result.class),
        Writer.file(new Path(sequenceFile.toURI())))) {

      for (Collection<? extends Cell> cells : records.asMap().values()) {
        Result row = Result.create(cells.toArray(new Cell[cells.size()]));
        writer.append(new ImmutableBytesWritable(row.getRow()), row);
      }
      writer.close();
    }
  }

  /**
   * Returns a {@link Set} of {@link Cell}s from the content of a Sequence File.
   */
  public static Set<? extends Cell> readCellsFromSource(HBaseImportOptions importOptions)
      throws IOException {
    BoundedSource<KV<ImmutableBytesWritable, Result>> source =
        HBaseImportIO.createSource(importOptions);
    Iterable<List<Cell>> cellsByRow =
        Iterables.transform(
            SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create()),
            new Function<KV<ImmutableBytesWritable, Result>, List<Cell>>() {
              @Override
              public List<Cell> apply(KV<ImmutableBytesWritable, Result> kv) {
                return kv.getValue().listCells();
              }
            });
    Set<Cell> cells = Sets.newHashSet();
    for (List<Cell> rowCells: cellsByRow) {
      cells.addAll(rowCells);
    }
    return cells;
  }
}
