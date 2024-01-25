/*
 * Copyright 2018 Google LLC
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

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.core.InternalExtensionOnly;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.Serializable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;

/**
 * @deprecated This is a Bigtable extension will be removed in the future. bigtable-hbase is meant
 *     to be a compatibility layer from HBase to Bigtable. Adding extensions is out of scope.
 */
@Deprecated
@InternalExtensionOnly
public class BigtableFilter extends FilterBase implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Filters.Filter filter;

  public BigtableFilter(Filters.Filter filter) {
    this.filter = filter;
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) {
    return ReturnCode.SKIP;
  }

  public Filters.Filter getFilter() {
    return filter;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof BigtableFilter)) {
      return false;
    }
    BigtableFilter other = (BigtableFilter) obj;
    return filter.toProto().equals(other.filter.toProto());
  }

  @Override
  public byte[] toByteArray() {
    return filter.toProto().toByteArray();
  }

  public static BigtableFilter parseFrom(final byte[] bytes) throws DeserializationException {
    try {
      return new BigtableFilter(FILTERS.fromProto(RowFilter.parseFrom(bytes)));
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
  }
}
