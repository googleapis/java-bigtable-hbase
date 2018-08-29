/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;

import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * An HBase {@link Filter} that wraps a Cloud Bigtable {@link Filters.Filter}. Generally, users
 * should opt for a pure HBase {@link Filter}.  There are complex cases where a Cloud Bigtable 
 * {@link Filters.Filter} can express a more robust expression than the HBase semantics, or can
 * be used to express an expression that's more performant than a translation of a complex
 * HBase {@link FilterList}.
 */
public class BigtableFilter extends FilterBase implements Serializable {

  private static final long serialVersionUID = 1L;

  private final RowFilter rowFilter;

  public BigtableFilter(Filters.Filter filter) {
    this.rowFilter = filter.toProto();
  }

  private BigtableFilter(RowFilter rowFilter) {
    this.rowFilter = rowFilter;
  }

  @Override
  public ReturnCode filterKeyValue(Cell arg0) throws IOException {
    return ReturnCode.SKIP;
  }

  public RowFilter getRowFilter() {
    return rowFilter;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof BigtableFilter)) {
      return false;
    }
    BigtableFilter other = (BigtableFilter) obj;
    return rowFilter.equals(other.rowFilter);
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return rowFilter.toByteArray();
  }

  public static BigtableFilter parseFrom(final byte[] bytes) throws DeserializationException {
    try {
      return new BigtableFilter(RowFilter.parseFrom(bytes));
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }
}
