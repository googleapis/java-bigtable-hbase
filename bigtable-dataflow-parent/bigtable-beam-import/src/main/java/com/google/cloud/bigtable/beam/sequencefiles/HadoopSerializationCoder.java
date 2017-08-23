package com.google.cloud.bigtable.beam.sequencefiles;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;


/**
 * A Beam coder that wraps a Hadoop Serialization.
 * @param <T> The type that will be serialized
 */
public class HadoopSerializationCoder<T> extends CustomCoder<T> {

  private final Class<T> type;
  private final Class<? extends Serialization<T>> serializationClass;
  private transient Serialization<T> serialization;

  /**
   * Constructs a new Coder
   * @param type The type that will be serialized.
   * @param serializationClass The Hadoop serialization class to delegate to.
   */
  @SuppressWarnings("unchecked")
  public HadoopSerializationCoder(final Class<T> type,
      Class<? extends Serialization<? super T>> serializationClass) {
    this.type = type;
    // Force an unchecked cast to workaround covariance issues
    this.serializationClass = (Class<? extends Serialization<T>>) serializationClass;
  }

  /**
   * Populate a possibly unserializable serialization instance.
   */
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();

    try {
      serialization = serializationClass.newInstance();
    } catch (IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(
          "Failed to deserialize " + HadoopSerializationCoder.class.getSimpleName());
    }
  }

  /**
   * Disable backwards compatibility with previous versions that were serialized
   * @throws InvalidObjectException
   */
  private void readObjectNoData() throws InvalidObjectException {
    throw new InvalidObjectException("Stream data required");
  }


  /** {@inheritDoc} */
  @Override
  public void encode(T value, OutputStream outStream) throws CoderException, IOException {
    Serializer<T> serializer = this.serialization.getSerializer(type);
    serializer.open(new UncloseableOutputStream(outStream));
    try {
      serializer.serialize(value);
    } finally {
      serializer.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public T decode(InputStream inStream) throws CoderException, IOException {
    Deserializer<T> deserializer = serialization.getDeserializer(type);
    deserializer.open(new UncloseableInputStream(inStream));
    try {
      return deserializer.deserialize(null);
    } finally {
      deserializer.close();
    }
  }

  /**
   * Simple wrapper to prevent the Hadoop serializers from closing a stream they don't own.
   */
  private static class UncloseableOutputStream extends FilterOutputStream {
    public UncloseableOutputStream(OutputStream delegate) {
      super(delegate);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      // noop
    }
  }

  /**
   * Simple wrapper to prevent the Hadoop serializers from closing a stream they don't own.
   */

  private static class UncloseableInputStream extends FilterInputStream {

    public UncloseableInputStream(InputStream delegate) {
      super(delegate);
    }

    @Override
    public void close() throws IOException {
      // noop
    }
  }
}
