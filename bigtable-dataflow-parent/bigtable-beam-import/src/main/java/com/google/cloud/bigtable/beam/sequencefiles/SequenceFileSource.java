package com.google.cloud.bigtable.beam.sequencefiles;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.primitives.UnsignedBytes;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.FileBasedSource;
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

public class SequenceFileSource<K, V> extends FileBasedSource<KV<K, V>> {
  private static final Log LOG = LogFactory.getLog(SequenceFileSource.class);

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final KvCoder<K,V> coder;
  private final List<Class<? extends Serialization<?>>> serializations;
  private static final long minBundleSize = SequenceFile.SYNC_INTERVAL; // TODO: make this configureable and figure out if it should be higher

  public SequenceFileSource(ValueProvider<String> fileOrPatternSpec, Class<K> keyClass,
      Coder<K> keyCoder, Class<V> valueClass, Coder<V> valueCoder, List<Class<? extends Serialization<?>>> serializations) {
    super(fileOrPatternSpec, minBundleSize);
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.coder = KvCoder.of(keyCoder,valueCoder);
    this.serializations = serializations;
  }

  SequenceFileSource(Metadata fileMetadata,
      long startOffset, long endOffset,
      Class<K> keyClass, Coder<K> keyCoder,
      Class<V> valueClass, Coder<V> valueCoder,
      List<Class<? extends Serialization<?>>> serializations) {
    super(fileMetadata, minBundleSize, startOffset, endOffset);
    this.keyClass = keyClass;
    this.valueClass = valueClass;
    this.coder = KvCoder.of(keyCoder,valueCoder);
    this.serializations = serializations;
  }

  @Override
  protected FileBasedSource<KV<K, V>> createForSubrangeOfFile(Metadata fileMetadata, long start,
      long end) {
    LOG.debug("Creating source for subrange: " + start + "-" + end);
    return new SequenceFileSource<>(fileMetadata,
        start, end,
        keyClass, coder.getKeyCoder(),
        valueClass, coder.getValueCoder(),
        serializations
    );
  }

  @Override
  protected FileBasedReader<KV<K, V>> createSingleFileReader(PipelineOptions options) {
    String[] names = new String[serializations.size()];
    for(int i=0; i<names.length; i++) {
      names[i] = serializations.get(i).getName();
    }

    return new SeqFileReader<>(this, keyClass, valueClass, names);
  }

  @Override
  public Coder<KV<K, V>> getDefaultOutputCoder() {
    return coder;
  }

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

      LOG.debug("startReading, offset: " + getCurrentSource().getStartOffset() + ", position: " + startOfNextRecord);
    }

    @Override
    public void close() throws IOException {
      super.close();
      reader.close();
    }

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
        value = (V) reader.getCurrentValue(value);
        record = KV.of(key, value);
      }

      isAtSplitPoint = isFirstRecord || reader.syncSeen();
      isFirstRecord = false;
      startOfNextRecord = reader.getPosition();

      return record != null;
    }

    @Override
    protected boolean isAtSplitPoint() throws NoSuchElementException {
      return isAtSplitPoint;
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      if (record == null) {
        throw new NoSuchElementException();
      }
      return startOfRecord;
    }

    @Override
    public KV<K, V> getCurrent() throws NoSuchElementException {
      if (record == null) {
        throw new NoSuchElementException();
      }
      return record;
    }
  }

  static class FileStream extends FSInputStream {

    private final SeekableByteChannel inner;
    private final ByteBuffer singleByteBuffer = ByteBuffer.allocate(1);

    public FileStream(SeekableByteChannel inner) {
      this.inner = inner;
    }

    @Override
    public void seek(long l) throws IOException {
      inner.position(l);
    }

    @Override
    public long getPos() throws IOException {
      return inner.position();
    }

    @Override
    public boolean seekToNewSource(long l) throws IOException {
      return false;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, length);
      return inner.read(byteBuffer);
    }

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
