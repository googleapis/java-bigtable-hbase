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

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
class HadoopSerializationCoder<T> extends CustomCoder<T> {

  private final Class<T> type;
  private final Class<? extends Serialization<T>> serializationClass;
  private transient Serialization<T> serialization;

  /**
   * Constructs a new Coder
   * @param type The type that will be serialized.
   * @param serializationClass The Hadoop serialization class to delegate to.
   */
  @SuppressWarnings("unchecked")
  HadoopSerializationCoder(final Class<T> type,
      Class<? extends Serialization<? super T>> serializationClass) {
    this.type = type;
    // Force an unchecked cast to workaround covariance issues
    this.serializationClass = (Class<? extends Serialization<T>>) serializationClass;
    try {
      this.serialization = (Serialization<T>) serializationClass.newInstance();
    } catch (InstantiationException|IllegalAccessException e) {
      throw new RuntimeException("Failed to instantiate the serialization class");
    }
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
