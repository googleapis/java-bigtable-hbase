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
package com.google.cloud.bigtable.hbase.filter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * Defines a filter that only returns cells whose version matches a start and end (both are
 * inclusive)
 */
public class TimestampRangeFilter extends FilterBase implements Serializable {

  private static final long serialVersionUID = 1L;
  private final long startTimestampInclusive;
  private final long endTimestampExclusive;

  public TimestampRangeFilter(long startTimestamp, long endTimestamp) {
    this.startTimestampInclusive = startTimestamp;
    this.endTimestampExclusive = endTimestamp;
  }

  public long getStartTimestampInclusive() {
    return startTimestampInclusive;
  }

  public long getEndTimestampExclusive() {
    return endTimestampExclusive;
  }

  @Override
  /**
   * This is for HBase compatibility, and will not be used for Cloud Bigtable
   */
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    long timestamp = cell.getTimestamp();
    if (this.startTimestampInclusive <= timestamp && timestamp < endTimestampExclusive) {
      return ReturnCode.INCLUDE;
    }
    return ReturnCode.SKIP;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof TimestampRangeFilter)) {
      return false;
    }
    TimestampRangeFilter other = (TimestampRangeFilter) obj;
    return startTimestampInclusive == other.startTimestampInclusive
        && endTimestampExclusive == other.endTimestampExclusive;
  }

  @Override
  public byte[] toByteArray() throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(this);
      out.flush();
      return bos.toByteArray();
    }
  }

  public static TimestampRangeFilter parseFrom(final byte [] bytes)
  throws DeserializationException {
    try (ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(bytes))){
      return (TimestampRangeFilter) is.readObject();
    } catch (Exception e) {
      throw new DeserializationException(e);
    }
  }
}
