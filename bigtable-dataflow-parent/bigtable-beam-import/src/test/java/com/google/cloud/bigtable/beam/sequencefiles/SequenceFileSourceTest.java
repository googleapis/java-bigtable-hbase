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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
public class SequenceFileSourceTest {
  @Rule
  public final TemporaryFolder workDir = new TemporaryFolder();

  @Test
  public void testSimpleWritable() throws IOException {
    Configuration config = new Configuration(false);

    List<KV<Text, Text>> data = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      data.add(KV.of(new Text("key" + i), new Text("value" + i)));
    }

    // Write data to read
    File targetFile = workDir.newFile();

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

    List<KV<Text, Text>> results = SourceTestUtils.readFromSource(source, null);
    assertThat(results, containsInAnyOrder(data.toArray()));
  }

  @Test
  public void testHBaseTypes() throws Exception {
    File targetFile = workDir.newFile();

    final List<KV<ImmutableBytesWritable, Result>> data = Lists.newArrayList();

    final int nRows = 10;
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

    // Verify
    List<KV<ImmutableBytesWritable, Result>> actual = SourceTestUtils.readFromSource(source, null);
    assertThat(actual, hasSize(data.size()));

    Collections.sort(actual, new Comparator<KV<ImmutableBytesWritable, Result>>() {
      @Override
      public int compare(KV<ImmutableBytesWritable, Result> o1,
          KV<ImmutableBytesWritable, Result> o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });
    for(int i=0; i < data.size(); i++) {
      KV<ImmutableBytesWritable, Result> expectedKv = data.get(i);
      KV<ImmutableBytesWritable, Result> actualKv = actual.get(i);

      assertEquals(expectedKv.getKey(), actualKv.getKey());
      assertEquals(actualKv.getValue().rawCells().length, expectedKv.getValue().rawCells().length);

      for(int j=0; j < expectedKv.getValue().rawCells().length; j++) {
        Cell expectedCell = expectedKv.getValue().rawCells()[j];
        Cell actualCell = actualKv.getValue().rawCells()[j];
        assertTrue(CellUtil.equals(expectedCell, actualCell));
        assertTrue(CellUtil.matchingValue(expectedCell, actualCell));
      }
    }
  }

  @Test
  public void testCompression() throws IOException {
    Configuration config = new Configuration(false);

    List<KV<Text, Text>> data = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      data.add(KV.of(new Text("key" + i), new Text("value" + i)));
    }

    for (CompressionType compressionType : CompressionType.values()) {

      // Write data to read
      File targetFile = workDir.newFile();

      try (Writer writer = SequenceFile.createWriter(config,
          Writer.file(new org.apache.hadoop.fs.Path(targetFile.toString())),
          Writer.keyClass(Text.class),
          Writer.valueClass(Text.class),
          Writer.compression(compressionType)

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

      List<KV<Text, Text>> results = SourceTestUtils.readFromSource(source, null);
      assertThat(results, containsInAnyOrder(data.toArray()));
    }
  }

  @Test
  public void testSplitAtFraction() throws Exception {
    Configuration config = new Configuration(false);

    int recordCount = 1000;

    List<KV<Text, Text>> data = Lists.newArrayList();
    for (int i = 0; i < recordCount; i++) {
      data.add(KV.of(new Text(String.format("key-%03d", i)), new Text(String.format("value-%03d", i))));
    }

    // Write data to read
    File targetFile = workDir.newFile();

    try (Writer writer = SequenceFile.createWriter(config,
        Writer.file(new org.apache.hadoop.fs.Path(targetFile.toString())),
        Writer.keyClass(Text.class),
        Writer.valueClass(Text.class),
        Writer.blockSize(1),
        Writer.compression(CompressionType.NONE)
    )) {
      int syncEvery = 10;
      int noSyncSince = 0;
      for (KV<Text, Text> kv : data) {
        writer.append(kv.getKey(), kv.getValue());
        writer.sync();
        noSyncSince++;
        if (syncEvery >= noSyncSince) {
          writer.sync();
          noSyncSince = 0;
        }
      }
    }

    // Setup the source with a single split
    SequenceFileSource<Text, Text> source = new SequenceFileSource<>(
        StaticValueProvider.of(targetFile.getAbsolutePath()),
        Text.class, WritableSerialization.class,
        Text.class, WritableSerialization.class,
        Long.MAX_VALUE
    );

    List<? extends FileBasedSource<KV<Text, Text>>> splits = source
        .split(targetFile.length() / 3, null);


//    for (FileBasedSource<KV<Text, Text>> subSource : splits) {
    FileBasedSource<KV<Text, Text>> subSource = splits.get(0);

      int items = SourceTestUtils.readFromSource(subSource, null).size();

      // Shouldn't split while unstarted.
      SourceTestUtils.assertSplitAtFractionFails(subSource, 0, 0.0, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, 0, 0.7, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(subSource, 1, 0.7, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(
          subSource, recordCount / 100, 0.7, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(
          subSource, recordCount / 10, 0.1, null);
//      SourceTestUtils.assertSplitAtFractionFails(
//          subSource, (recordCount / 10) + 1, 0.1, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, recordCount / 3, 0.3, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, items, 0.9, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, items, 1.0, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(subSource, items, 0.999, null);

    }
//  }
}

