package com.google.cloud.bigtable.beam.sequencefiles;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.cloud.bigtable.beam.coders.HBaseResultCoder;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.FileBasedSource.FileBasedReader;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.hadoop.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SequenceFileSourceTest {

  @Rule
  public final TemporaryFolder workDir = new TemporaryFolder();

  @Test
  public void testReader() throws IOException {
    Configuration config = new Configuration(false);
    File targetFile = new File(workDir.getRoot(), "file.seq");

    try (Writer writer = SequenceFile.createWriter(config,
        Writer.file(new org.apache.hadoop.fs.Path(targetFile.toString())),
        Writer.keyClass(Text.class),
        Writer.valueClass(Text.class)
    )) {
      writer.append(new Text("key1"), new Text("value1"));
      writer.append(new Text("key2"), new Text("value2"));
    }

    SequenceFileSource<Text, Text> source = new SequenceFileSource<>(
        FileSystems.matchSingleFileSpec(targetFile.getAbsolutePath()),
        0, targetFile.length(),
        Text.class, WritableCoder.of(Text.class),
        Text.class, WritableCoder.of(Text.class),
        Collections.<Class<? extends Serialization<?>>>emptyList()
    );

    try (FileBasedReader<KV<Text,Text>> reader = source
        .createSingleFileReader(PipelineOptionsFactory.create())) {

      assertThat(reader.start(), equalTo(true));
      assertThat(reader.getCurrent(), equalTo(KV.of(new Text("key1"), new Text("value1"))));

      assertThat(reader.advance(), equalTo(true));
      assertThat(reader.getCurrent(), equalTo(KV.of(new Text("key2"), new Text("value2"))));

      assertThat(reader.advance(), equalTo(false));
    }
  }

  @Test
  public void testNonWritable() throws IOException {
    File targetFile = new File(workDir.getRoot(), "file.seq");

    ImmutableBytesWritable key = new ImmutableBytesWritable("key1".getBytes());
    Result value = Result.create(Collections.singletonList(
        CellUtil
            .createCell("k1".getBytes(), "cf".getBytes(), "qualifier".getBytes(), 123456, Type.Put,
                "value".getBytes(), "tags".getBytes())
        )
    );
    List<Class<? extends Serialization<?>>> serializations = Arrays.<Class<? extends Serialization<?>>>asList(
        ResultSerialization.class, WritableSerialization.class
    );
    String[] serializationNames = new String[serializations.size()];
    for (int i = 0; i < serializations.size(); i++) {
      serializationNames[i] = serializations.get(i).getName();
    }


    // Write the file
    Configuration config = new Configuration(false);
    config.setStrings("io.serializations", serializationNames);

    try (Writer writer = SequenceFile.createWriter(config,
        Writer.file(new org.apache.hadoop.fs.Path(targetFile.toString())),
        Writer.keyClass(ImmutableBytesWritable.class),
        Writer.valueClass(Result.class)
    )) {
      writer.append(key, value);
    }

    // Read the file
    SequenceFileSource<ImmutableBytesWritable, Result> source = new SequenceFileSource<>(
        FileSystems.matchSingleFileSpec(targetFile.getAbsolutePath()),
        0, targetFile.length(),
        ImmutableBytesWritable.class, WritableCoder.of(ImmutableBytesWritable.class),
        Result.class, HBaseResultCoder.of(), serializations
    );


    try (FileBasedReader<KV<ImmutableBytesWritable,Result>> reader = source
        .createSingleFileReader(PipelineOptionsFactory.create())) {


      assertThat(reader.start(), equalTo(true));
      assertThat(reader.getCurrent().getKey(), equalTo(key));
      assertThat(reader.getCurrent().getValue().rawCells(), equalTo(value.rawCells()));

      assertThat(reader.advance(), equalTo(false));
    }
  }
}