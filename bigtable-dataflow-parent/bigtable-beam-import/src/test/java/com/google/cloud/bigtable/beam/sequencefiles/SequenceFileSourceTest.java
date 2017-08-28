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
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SequenceFileSourceTest {

  @Rule
  public final TemporaryFolder workDir = new TemporaryFolder();

  public SequenceFileSourceTest() throws IOException {
  }

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
    for (int i = 0; i < data.size(); i++) {
      KV<ImmutableBytesWritable, Result> expectedKv = data.get(i);
      KV<ImmutableBytesWritable, Result> actualKv = actual.get(i);

      assertEquals(expectedKv.getKey(), actualKv.getKey());
      assertEquals(actualKv.getValue().rawCells().length, expectedKv.getValue().rawCells().length);

      for (int j = 0; j < expectedKv.getValue().rawCells().length; j++) {
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
    File targetFile = generateTextData(100, 10);

    // Setup the source with a single split
    SequenceFileSource<Text, Text> source = new SequenceFileSource<>(
        StaticValueProvider.of(targetFile.getAbsolutePath()),
        Text.class, WritableSerialization.class,
        Text.class, WritableSerialization.class,
        Long.MAX_VALUE
    );

    List<? extends FileBasedSource<KV<Text, Text>>> splits = source
        .split(targetFile.length(), null);

    for (FileBasedSource<KV<Text, Text>> subSource : splits) {
      int items = SourceTestUtils.readFromSource(subSource, null).size();

      // Shouldn't split while unstarted.
      SourceTestUtils.assertSplitAtFractionFails(subSource, 0, 0.0, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, 0, 0.7, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(subSource, 1, 0.7, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(
          subSource, 10, 0.7, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(
          subSource, 1, 0.1, null);
      // The split should fail after reading 11 items, (the start of the 11th item is at 10% of the
      // file. However because of syncs, 0.1 will put the cursor 20 bytes ahead of the current
      // position. So we pad the test with an extra item
      SourceTestUtils.assertSplitAtFractionFails(
          subSource, 11 + 1, 0.1, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, 33, 0.3, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, items, 0.9, null);
      SourceTestUtils.assertSplitAtFractionFails(subSource, items, 1.0, null);
      SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent(subSource, items, 0.999, null);

    }
  }

  @Test
  public void testSplitAtFractionNonoverlapping() throws Exception {
    File targetFile = generateTextData(100, 10);
    // Setup the source with a single split
    SequenceFileSource<Text, Text> source = new SequenceFileSource<>(
        StaticValueProvider.of(targetFile.getAbsolutePath()),
        Text.class, WritableSerialization.class,
        Text.class, WritableSerialization.class,
        Long.MAX_VALUE
    );

    List<? extends FileBasedSource<KV<Text, Text>>> splits = source
        .split(targetFile.length(), null);

    for (FileBasedSource<KV<Text, Text>> subSource : splits) {
      SourceTestUtils.assertSplitAtFractionFails(
          subSource, 11 + 1, 0.1, null);

      BoundedReader<KV<Text, Text>> reader = subSource.createReader(null);
      List<KV<Text, Text>> before = SourceTestUtils.readNItemsFromUnstartedReader(reader, 11);
      List<KV<Text, Text>> after = SourceTestUtils.readRemainingFromReader(reader, true);
      assertEquals(100, before.size() + after.size());
      assertNotEquals(after.get(0), before.get(before.size() - 1));
    }
  }

  @Test
  public void testSplitAtFractionExhaustive() throws Exception {
    File targetFile = generateTextData(100, 10);

    // Setup the source with a single split
    SequenceFileSource<Text, Text> source = new SequenceFileSource<>(
        StaticValueProvider.of(targetFile.getAbsolutePath()),
        Text.class, WritableSerialization.class,
        Text.class, WritableSerialization.class,
        Long.MAX_VALUE
    );

    SourceTestUtils.assertSplitAtFractionExhaustive(source, null);
  }

  @Test
  public void testSplitPoints() throws Exception {
    File targetFile = generateTextData(100, 10);

    // Setup the source with a single split
    SequenceFileSource<Text, Text> source = new SequenceFileSource<>(
        StaticValueProvider.of(targetFile.getAbsolutePath()),
        Text.class, WritableSerialization.class,
        Text.class, WritableSerialization.class,
        Long.MAX_VALUE
    );

    List<? extends FileBasedSource<KV<Text, Text>>> splits = source
        .split(targetFile.length(), null);

    for (FileBasedSource<KV<Text, Text>> subSource : splits) {
      BoundedReader<KV<Text, Text>> origReader = subSource.createReader(null);
      assertThat(origReader, instanceOf(SequenceFileSource.SeqFileReader.class));

      SequenceFileSource.SeqFileReader<Text, Text> reader = (SequenceFileSource.SeqFileReader<Text, Text>) origReader;

      reader.start();
      assertTrue(reader.isAtSplitPoint());
      SourceTestUtils.readNItemsFromStartedReader(reader, 9);

      for (int i = 0; i < 9; i++) {
        reader.readNextRecord();
        assertTrue(reader.isAtSplitPoint());

        SourceTestUtils.readNItemsFromStartedReader(reader, 9);
      }
    }
  }

  @Test
  public void testAllRead() throws Exception {
    File targetFile = generateTextData(10, 10);
    // Setup the source with a single split
    SequenceFileSource<Text, Text> source = new SequenceFileSource<>(
        StaticValueProvider.of(targetFile.getAbsolutePath()),
        Text.class, WritableSerialization.class,
        Text.class, WritableSerialization.class,
        Long.MAX_VALUE
    );

    List<? extends FileBasedSource<KV<Text, Text>>> splits = source
        .split(targetFile.length(), null);

    List<KV<Text, Text>> results = SourceTestUtils.readFromSource(splits.get(0), null);
    assertThat(results, hasSize(10));
  }

  private File generateTextData(int recordCount, int syncInterval) throws IOException {
    Configuration config = new Configuration(false);

    List<KV<Text, Text>> data = Lists.newArrayList();
    for (int i = 0; i < recordCount; i++) {
      data.add(KV.of(new Text(String.format("key-%010d", i)),
          new Text(String.format("value-%010d", i))));
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
      int noSyncCount = 0;
      for (KV<Text, Text> kv : data) {
        writer.append(kv.getKey(), kv.getValue());
        noSyncCount++;
        if (noSyncCount >= syncInterval) {
          writer.sync();
          noSyncCount = 0;
        }
      }
    }

    return targetFile;
  }
}

