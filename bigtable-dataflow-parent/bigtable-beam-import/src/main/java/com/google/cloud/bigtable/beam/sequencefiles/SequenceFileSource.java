package com.google.cloud.bigtable.beam.sequencefiles;

import static com.google.common.base.Preconditions.checkState;

import avro.shaded.com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A {@link FileBasedSource} that can read hadoop's {@link SequenceFile}s.
 *
 * @param <K> The type of the {@link SequenceFile} key.
 * @param <V> The type of the {@link SequenceFile} value.
 */
public final class SequenceFileSource<K, V> extends FileBasedSource<KV<K, V>> {

  private static final Log LOG = LogFactory.getLog(SequenceFileSource.class);

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final Class<? extends Serialization<? super K>> keySerializationClass;
  private final Class<? extends Serialization<? super V>> valueSerializationClass;
  private final KvCoder<K, V> coder;
  private static final long minBundleSize = SequenceFile.SYNC_INTERVAL; // TODO: make this configureable and figure out if it should be higher

  /**
   * Constructs a new top level source.
   *
   * @param fileOrPatternSpec The path or pattern of the file(s) to read.
   * @param keyClass The {@link Class} of the key.
   * @param keySerialization The {@link Class} of the hadoop {@link org.apache.hadoop.io.serializer.Serialization}
   *        to use for the key.
   * @param valueClass The {@link Class} of the value.
   * @param valueSerialization The {@link Class} of the hadoop {@link
   * org.apache.hadoop.io.serializer.Serialization} to use for the value.
   */
  public SequenceFileSource(ValueProvider<String> fileOrPatternSpec,
      Class<K> keyClass,
      Class<? extends Serialization<? super K>> keySerialization,
      Class<V> valueClass,
      Class<? extends Serialization<? super V>> valueSerialization) {
    super(fileOrPatternSpec, minBundleSize);

    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.keySerializationClass = keySerialization;
    this.valueSerializationClass = valueSerialization;
    this.coder = KvCoder.of(
        new HadoopSerializationCoder<>(keyClass, keySerialization),
        new HadoopSerializationCoder<>(valueClass, valueSerialization)
    );
  }

  /**
   * Constructs a subsource for a given range.
   *
   * @param fileMetadata specification of the file represented by the {@link SequenceFileSource}, in
   * suitable form for use with {@link FileSystems#match(List)}.
   * @param startOffset starting byte offset.
   * @param endOffset ending byte offset. If the specified value {@code >= #getMaxEndOffset()} it
   *        implies {@code #getMaxEndOffSet()}.
   * @param keyClass The {@link Class} of the key.
   * @param keySerialization The {@link Class} of the hadoop {@link org.apache.hadoop.io.serializer.Serialization}
   *        to use for the key.
   * @param valueClass The {@link Class} of the value.
   * @param valueSerialization The {@link Class} of the hadoop {@link
   *        org.apache.hadoop.io.serializer.Serialization} to use for the value.
   */
  SequenceFileSource(Metadata fileMetadata,
      long startOffset, long endOffset,
      Class<K> keyClass,
      Class<? extends Serialization<? super K>> keySerialization,
      Class<V> valueClass,
      Class<? extends Serialization<? super V>> valueSerialization,
      KvCoder<K, V> coder) {
    super(fileMetadata, minBundleSize, startOffset, endOffset);
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.keySerializationClass = keySerialization;
    this.valueSerializationClass = valueSerialization;
    this.coder = coder;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected FileBasedSource<KV<K, V>> createForSubrangeOfFile(Metadata fileMetadata, long start,
      long end) {
    LOG.debug("Creating source for subrange: " + start + "-" + end);
    return new SequenceFileSource<>(fileMetadata,
        start, end,
        keyClass,
        keySerializationClass, valueClass,
        valueSerializationClass,
        coder
    );
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected FileBasedReader<KV<K, V>> createSingleFileReader(PipelineOptions options) {
    Set<String> serializationNames = Sets.newHashSet(
        keySerializationClass.getName(),
        valueSerializationClass.getName()
    );
    return new SeqFileReader<>(this, keyClass, valueClass,
        serializationNames.toArray(new String[serializationNames.size()]));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Coder<KV<K, V>> getDefaultOutputCoder() {
    return coder;
  }

  /**
   * A {@link FileBasedSource.FileBasedReader} for reading records from a {@link SequenceFile}.
   *
   * @param <K> The type of the record keys.
   * @param <V> The type of the record values.
   */
  static class SeqFileReader<K, V> extends FileBasedReader<KV<K, V>> {

    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final String[] serializationNames;

    private SequenceFile.Reader reader;

    // Sync is consumed during startReading(), so we need to track that for the first call of
    // readNextRecord
    private boolean isFirstRecord;
    private boolean isAtSplitPoint;
    private boolean eof;

    private long startOfNextRecord;
    private long startOfRecord;
    private KV<K, V> record;

    public SeqFileReader(FileBasedSource<KV<K, V>> source, Class<K> keyClass, Class<V> valueClass,
        String[] serializationNames) {
      super(source);
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.serializationNames = serializationNames;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      checkState(channel instanceof SeekableByteChannel,
          "%s only supports reading from a SeekableByteChannel",
          SequenceFileSource.class.getSimpleName()
      );

      SeekableByteChannel seekableByteChannel = (SeekableByteChannel) channel;
      FileStream fileStream = new FileStream(seekableByteChannel);
      FSDataInputStream fsDataInputStream = new FSDataInputStream(fileStream);

      // Construct the underlying SequenceFile.Reader
      Configuration configuration = new Configuration(false);
      if (serializationNames.length > 0) {
        configuration.setStrings("io.serializations", serializationNames);
      }

      reader = new SequenceFile.Reader(configuration,
          SequenceFile.Reader.stream(fsDataInputStream));

      // Seek to the start of the next closest sync point
      try {
        reader.sync(getCurrentSource().getStartOffset());
      } catch (EOFException e) {
        LOG.warn("Found EOF when starting to read: " + getCurrentSource().getStartOffset());
        eof = true;
      }

      // Prep for the next readNextRecord() call
      startOfNextRecord = reader.getPosition();
      isFirstRecord = true;

      LOG.debug("startReading, offset: " + getCurrentSource().getStartOffset() + ", position: "
          + startOfNextRecord);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
      super.close();
      reader.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean readNextRecord() throws IOException {
      if (eof) {
        return false;
      }

      K key = ReflectionUtils.newInstance(keyClass, null);
      V value = ReflectionUtils.newInstance(valueClass, null);

      startOfRecord = startOfNextRecord;

      try {
        eof = reader.next(key) == null;
      } catch (EOFException e) {
        eof = true;
      }

      if (eof) {
        record = null;
      } else {
        value = readCurrentValueUnchecked(value);
        record = KV.of(key, value);
      }

      isAtSplitPoint = isFirstRecord || reader.syncSeen();
      isFirstRecord = false;
      startOfNextRecord = reader.getPosition();

      return record != null;
    }

    @SuppressWarnings("unchecked")
    private V readCurrentValueUnchecked(V value) throws IOException {
      return (V) reader.getCurrentValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isAtSplitPoint() throws NoSuchElementException {
      return isAtSplitPoint;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      if (record == null) {
        throw new NoSuchElementException();
      }
      return startOfRecord;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public KV<K, V> getCurrent() throws NoSuchElementException {
      if (record == null) {
        throw new NoSuchElementException();
      }
      return record;
    }
  }


  /**
   * Adapter to convert a Beam {@link SeekableByteChannel} to hadoop's {@link FSDataInputStream}.
   */
  static class FileStream extends FSInputStream {

    private final SeekableByteChannel inner;
    private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);

    public FileStream(SeekableByteChannel inner) {
      this.inner = inner;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(long l) throws IOException {
      inner.position(l);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getPos() throws IOException {
      return inner.position();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean seekToNewSource(long l) throws IOException {
      return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
      return inner.read(byteBuffer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() throws IOException {
      int numRead = 0;

      singleByteBuffer.clear();
      while (numRead == 0) {
        numRead = inner.read(singleByteBuffer);
      }

      if (numRead == -1) {
        return -1;
      }

      return UnsignedBytes.toInt(singleByteBuffer.get(0));
    }
  }
}
