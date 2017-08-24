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
package com.google.cloud.bigtable.beam.sequencefiles;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

public class SequenceFileSourceTest {

  @Rule
  public TestPipeline readPipeline = TestPipeline.create();

  @Rule
  public final TemporaryFolder workDir = new TemporaryFolder();

  @Test
  @Category(NeedsRunner.class)
  public void testReader() throws IOException {
    Configuration config = new Configuration(false);

    List<KV<Text, Text>> data = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      data.add(KV.of(new Text("key" + i), new Text("value" + i)));
    }

    // Write data to read
    File targetFile = new File(workDir.getRoot(), "file.seq");

    try (Writer writer = SequenceFile.createWriter(config,
        Writer.file(new org.apache.hadoop.fs.Path(targetFile.toString())),
        Writer.keyClass(Text.class),
        Writer.valueClass(Text.class)
    )) {
      for (KV<Text, Text> kv : data) {
        writer.append(kv.getKey(), kv.getValue());
      }
    }

    // Setup the source
    SequenceFileSource<Text, Text> source = new SequenceFileSource<>(
        StaticValueProvider.of(targetFile.getAbsolutePath()),
        Text.class, WritableSerialization.class,
        Text.class, WritableSerialization.class,
        SequenceFile.SYNC_INTERVAL
    );

    PAssert
        .that(readPipeline.apply(Read.from(source)))
        .containsInAnyOrder(data);

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testNonWritable() throws IOException {
    File targetFile = new File(workDir.getRoot(), "file.seq");

    final List<KV<ImmutableBytesWritable, Result>> data = Lists.newArrayList();

    final int nRows = 100;
    for (int i = 0; i < nRows; i++) {
      String keyStr = String.format("%03d", i);

      ImmutableBytesWritable rowKey = new ImmutableBytesWritable(
          keyStr.getBytes());

      Result value = Result.create(Collections.singletonList(
          CellUtil.createCell(
              keyStr.getBytes(),
              ("family" + i).getBytes(),
              ("qualifier" + i).getBytes(),
              123456,
              Type.Put.getCode(),
              ("value" + i).getBytes()
          )));
      data.add(
          KV.of(rowKey, value)
      );
    }

    // Write the file
    Configuration config = new Configuration(false);
    config.setStrings("io.serializations",
        ResultSerialization.class.getName(),
        WritableSerialization.class.getName()
    );

    try (Writer writer = SequenceFile.createWriter(config,
        Writer.file(new org.apache.hadoop.fs.Path(targetFile.toString())),
        Writer.keyClass(ImmutableBytesWritable.class),
        Writer.valueClass(Result.class)
    )) {
      for (KV<ImmutableBytesWritable, Result> kv : data) {
        writer.append(kv.getKey(), kv.getValue());
      }
    }

    // Read the file
    SequenceFileSource<ImmutableBytesWritable, Result> source = new SequenceFileSource<>(
        StaticValueProvider.of(targetFile.getAbsolutePath()),
        ImmutableBytesWritable.class, WritableSerialization.class,
        Result.class, ResultSerialization.class,
        SequenceFile.SYNC_INTERVAL
    );

    PAssert
        .that(readPipeline.apply(Read.from(source)))
        .satisfies(new ResultVerifier(nRows));

    readPipeline.run();
  }

  static class ResultVerifier extends SimpleFunction<Iterable<KV<ImmutableBytesWritable, Result>>, Void> {
    final int nRows;

    ResultVerifier(int nRows) {
      this.nRows = nRows;
    }

    @Override
    public Void apply(Iterable<KV<ImmutableBytesWritable, Result>> input) {
      ArrayList<KV<ImmutableBytesWritable, Result>> results = Lists.newArrayList(input);
      Collections.sort(results, new Comparator<KV<ImmutableBytesWritable, Result>>() {
        @Override
        public int compare(KV<ImmutableBytesWritable, Result> o1, KV<ImmutableBytesWritable, Result> o2) {
          return o1.getKey().compareTo(o2.getKey());
        }
      });

      for(int i=0; i<nRows; i++) {
        String keyStr = String.format("%03d", i);

        ImmutableBytesWritable key = results.get(i).getKey();
        Result result = results.get(i).getValue();
        Cell cell = result.rawCells()[0];

        Assert.assertEquals(keyStr, new String(key.copyBytes()));
        Assert.assertEquals(keyStr, new String(result.getRow()));
        Assert.assertEquals(keyStr, new String(CellUtil.cloneRow(cell)));
        Assert.assertEquals("family"+i, new String(CellUtil.cloneFamily(cell)));
        Assert.assertEquals("qualifier" +i, new String(CellUtil.cloneQualifier(cell)));
        Assert.assertEquals(123456, cell.getTimestamp());
        Assert.assertEquals("value"+i, new String(CellUtil.cloneValue(cell)));
        Assert.assertEquals(Type.Put.getCode(), cell.getTypeByte());
      }
      return null;
    }
  }
}

