/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.beam.validation;

import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.RangeHash;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Coder used by beam to encode/decode @{@link RangeHash} objects. */
public class RangeHashCoder extends Coder<RangeHash> {

  public static Coder<RangeHash> of() {
    return new RangeHashCoder();
  }

  @Override
  public void encode(RangeHash value, OutputStream outStream) throws IOException {
    if (value == null) {
      throw new CoderException("Can not encode null objects.");
    }
    DataOutputStream dataOutputStream = new DataOutputStream(outStream);
    // RangeHash fields can never be null.
    value.startInclusive.write(dataOutputStream);
    value.stopExclusive.write(dataOutputStream);
    value.hash.write(dataOutputStream);
  }

  @Override
  public RangeHash decode(InputStream inStream) throws IOException {
    DataInputStream dataInputStream = new DataInputStream(inStream);

    ImmutableBytesWritable startInclusive = new ImmutableBytesWritable();
    startInclusive.readFields(dataInputStream);

    ImmutableBytesWritable stopExclusive = new ImmutableBytesWritable();
    stopExclusive.readFields(dataInputStream);

    ImmutableBytesWritable hash = new ImmutableBytesWritable();
    hash.readFields(dataInputStream);

    return RangeHash.of(startInclusive, stopExclusive, hash);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.emptyList();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    // This is a deterministic coder as it writes the byte[] in order.
  }

  /**
   * !!! DO NOT DELETE !!!
   *
   * <p>See readObjectNoData method in:
   * https://docs.oracle.com/javase/7/docs/platform/serialization/spec/input.html#6053.
   *
   * <p>Disable backwards compatibility with previous versions that were serialized.
   *
   * @throws InvalidObjectException
   */
  @SuppressWarnings("unused")
  private void readObjectNoData() throws InvalidObjectException {
    throw new InvalidObjectException("Hash data required");
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof RangeHashCoder;
  }

  @Override
  public int hashCode() {
    return RangeHashCoder.class.hashCode();
  }
}
