/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc.scanner;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import com.google.appengine.repackaged.com.google.common.base.Objects;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.protobuf.ByteString;

/**
 * <p>This class stores values from {@link com.google.bigtable.v2.ReadRowsResponse} objects and
 * represents a single row.</p>
 *
 * @author tyagihas
 * @version $Id: $Id
 */
public class SimpleRow implements Serializable {

  private static final long serialVersionUID = 1L;

  public final static class SimpleCell implements Serializable {

    private static final long serialVersionUID = 1L;

    public final static class Builder {
      private String family;
      private ByteString qualifier;
      private long timestamp;
      private ByteString value;
      private List<String> labels;

      public Builder withFamily(String family) {
        this.family = family;
        return this;
      }

      public Builder withQualifier(ByteString qualifier) {
        this.qualifier = qualifier;
        return this;
      }

      public Builder withTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      public Builder withValue(ByteString value) {
        this.value = value;
        return this;
      }

      public Builder withLabels(List<String> labels) {
        if (labels == null || labels.isEmpty() ) {
          return this;
        }
        this.labels = labels;
        return this;
      }

      public SimpleCell build() {
        return new SimpleCell(family, qualifier, timestamp, value, labels);
      }
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    private final String family;
    private final ByteString qualifier;
    private final long timestamp;
    private final ByteString value;
    private final List<String> labels;

    private SimpleCell(String family, ByteString qualifier, long timestamp, ByteString value,
        List<String> labels) {
      this.family = family;
      this.qualifier = qualifier;
      this.timestamp = timestamp;
      this.value = value;
      this.labels = labels == null ? Collections.<String> emptyList() : labels;
    }

    public String getFamily() {
      return family;
    }

    public ByteString getQualifier() {
      return qualifier;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public ByteString getValue() {
      return value;
    }

    public List<String> getLabels() {
      return labels;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof SimpleCell)) {
        return false;
      }
      SimpleCell other = (SimpleCell) obj;
      return Objects.equal(family, other.family) &&
          Objects.equal(qualifier, other.qualifier) &&
          timestamp == other.timestamp &&
          Objects.equal(value, other.value) &&
          Objects.equal(labels, other.labels);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("family", family)
          .add("qualifier", qualifier)
          .add("timestamp", timestamp)
          .add("value", value)
          .add("labels", labels)
          .toString();
    }

  }

  public static class FamilyColumnOrdering extends Ordering<SimpleCell> {
    public static final FamilyColumnOrdering DEFAULT_ORDERING = new FamilyColumnOrdering();

    private FamilyColumnOrdering() {}

    @Override
    public int compare(SimpleCell left, SimpleCell right) {
      int result = left.family.compareTo(right.family);
      if (result != 0) {
        return result;
      }
      result = left.qualifier.asReadOnlyByteBuffer().compareTo(right.qualifier.asReadOnlyByteBuffer());
      if (result != 0) {
        return result;
      }
      // reverse timestamp ordering
      result = Long.compare(right.timestamp, left.timestamp);
      if (result != 0) {
        return result;
      }
      result = Integer.compare(left.getLabels().size(), right.getLabels().size());
      if (result != 0) {
        return result;
      }
      return 0;
    }
  }

  public static final class Builder {
    private ByteString rowKey = null;
    private final ImmutableList.Builder<SimpleCell> listBuilder;

    private Builder() {
      listBuilder = new ImmutableList.Builder<SimpleCell>();
    }

    public Builder withRowKey(ByteString rowKey) {
      Preconditions.checkNotNull(rowKey, "Row Key can not be null");
      this.rowKey = rowKey;
      return this;
    }

    public Builder addCell(String family, ByteString qualifier, long timestamp, ByteString value,
        List<String> labels) {
      return addCell(new SimpleCell(family, qualifier, timestamp, value, labels));
    }

    public Builder addCell(String family, ByteString qualifier, long timestamp, ByteString value) {
      return addCell(new SimpleCell(family, qualifier, timestamp, value, null));
    }

    public Builder addCell(SimpleCell column) {
      Preconditions.checkNotNull(column, "column can not be null");
      listBuilder.add(column);
      return this;
    }

    public SimpleRow build() {
      return new SimpleRow(rowKey, listBuilder.build());
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final ByteString rowKey;
  private final ImmutableList<SimpleCell> cells;

  public SimpleRow(ByteString rowKey, ImmutableList<SimpleCell> cells) {
    this.rowKey = rowKey;
    this.cells = cells;
  }

  public ByteString getRowKey() {
    return rowKey;
  }

  public List<SimpleCell> getCells() {
    return cells;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SimpleRow)) {
      return false;
    }
    SimpleRow other = (SimpleRow) obj;
    return Objects.equal(rowKey, other.rowKey) &&
        Objects.equal(cells, other.getCells());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("rowKey", rowKey)
        .add("cells", cells)
        .toString();
  }
}