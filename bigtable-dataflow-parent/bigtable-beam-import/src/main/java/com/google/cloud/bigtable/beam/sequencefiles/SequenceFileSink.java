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

import com.google.common.collect.Sets;
import com.sun.istack.internal.NotNull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.Serialization;

/**
 * {@link FileBasedSink} for {@link SequenceFile}s.
 *
 * @param <K> The type of the {@link SequenceFile} key.
 * @param <V> The type of the {@link SequenceFile} value.
 */
class SequenceFileSink<K,V> extends FileBasedSink<KV<K, V>> {
  private static final Log LOG = LogFactory.getLog(SequenceFileSink.class);

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final String[] serializationNames;

  /**
   * Constructs the sink.
   */
  SequenceFileSink(
      ValueProvider<ResourceId> baseOutputDirectoryProvider,
      FilenamePolicy filenamePolicy,
      Class<K> keyClass, Class<? extends Serialization<? super K>> keySerializationClass,
      Class<V> valueClass, Class<? extends Serialization<? super V>> valueSerializationClass) {
    super(baseOutputDirectoryProvider, filenamePolicy, CompressionType.UNCOMPRESSED);

    this.keyClass = keyClass;
    this.valueClass = valueClass;

    Set<String> serializationNameSet = Sets.newHashSet();
    serializationNameSet.add(keySerializationClass.getName());
    serializationNameSet.add(valueSerializationClass.getName());

    serializationNames = serializationNameSet.toArray(new String[serializationNameSet.size()]);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public WriteOperation<KV<K, V>> createWriteOperation() {
    return new SeqFileWriteOperation<>(this, keyClass, valueClass, serializationNames);
  }


  /**
   * Specialized operation that manages the process of writing to {@link SequenceFileSink}.
   * See {@link WriteOperation} for more details.
   *
   * @param <K> The type of the {@link SequenceFile} key.
   * @param <V> The type of the {@link SequenceFile} value.
   */
  private static class SeqFileWriteOperation<K,V> extends WriteOperation<KV<K,V>> {
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String[] serializationNames;

    /**
     * Constructs a SeqFileWriteOperation using the default strategy for generating a temporary
     * directory from the base output filename.
     *
     * <p>Default is a uniquely named sibling of baseOutputFilename, e.g. if baseOutputFilename is
     * /path/to/foo, the temporary directory will be /path/to/temp-beam-foo-$date.
     *
     * @param sink The {@link SequenceFileSink} that will be used to configure this write operation.
     * @param keyClass The class of the {@link SequenceFile} key.
     * @param valueClass The class of the {@link SequenceFile} value.
     * @param serializationNames A list of {@link Serialization} class names.
     */
    SeqFileWriteOperation(SequenceFileSink<K, V> sink, Class<K> keyClass,
        Class<V> valueClass, String[] serializationNames) {
      super(sink);
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.serializationNames = serializationNames;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Writer<KV<K, V>> createWriter() throws Exception {
      return new SeqFileWriter<>(this);
    }
  }

  /**
   * Wrapper for {@link SequenceFile.Writer} that adapts hadoop's {@link SequenceFile} api to Beam's
   * {@link org.apache.beam.sdk.io.FileBasedSink.Writer} api.
   *
   * @param <K> The type of the {@link SequenceFile} key.
   * @param <V> The type of the {@link SequenceFile} value.
   */
  private static class SeqFileWriter<K,V> extends Writer<KV<K,V>> {
    private final SeqFileWriteOperation<K,V> writeOperation;
    private SequenceFile.Writer sequenceFile;
    private final AtomicLong counter = new AtomicLong();

    /**
     * Constructs a new {@link SeqFileWriter}.
     *
     * @param writeOperation The parent operation.
     */
    SeqFileWriter(SeqFileWriteOperation<K, V> writeOperation) {
      super(writeOperation, MimeTypes.BINARY);
      this.writeOperation = writeOperation;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      LOG.info("Opening new writer");

      Configuration configuration = new Configuration(false);
      configuration.setStrings("io.serializations", writeOperation.serializationNames);

      FSDataOutputStream outputStream = new FSDataOutputStream(new OutputStreamWrapper(channel), new Statistics("dataflow"));
      sequenceFile = SequenceFile.createWriter(configuration,
          SequenceFile.Writer.stream(outputStream),
          SequenceFile.Writer.keyClass(writeOperation.keyClass),
          SequenceFile.Writer.valueClass(writeOperation.valueClass),
          SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
      );

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void finishWrite() throws Exception {
      sequenceFile.hflush();
      sequenceFile.close();
      super.finishWrite();
      LOG.info("Closing writer with " + counter.get() + " items");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(KV<K, V> value) throws Exception {
      counter.incrementAndGet();
      sequenceFile.append(value.getKey(), value.getValue());
    }
  }


  /**
   * Adapter to allow Hadoop's {@link SequenceFile} to write to Beam's {@link WritableByteChannel}.
   */
  static class OutputStreamWrapper extends OutputStream {
    private final WritableByteChannel inner;
    private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);

    /**
     * Constructs a new {@link OutputStreamWrapper}.
     *
     * @param inner An instance of Beam's {@link WritableByteChannel}.
     */
    OutputStreamWrapper(WritableByteChannel inner) {
      this.inner = inner;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      int written = 0;

      ByteBuffer byteBuffer = ByteBuffer.wrap(b, off, len);

      while(written < len) {
        byteBuffer.position(written + off);
        written += this.inner.write(byteBuffer);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(int b) throws IOException {
      singleByteBuffer.clear();
      singleByteBuffer.put((byte)b);

      int written = 0;

      while(written == 0) {
        singleByteBuffer.position(0);
        written = this.inner.write(singleByteBuffer);
      }
    }
  }
}
