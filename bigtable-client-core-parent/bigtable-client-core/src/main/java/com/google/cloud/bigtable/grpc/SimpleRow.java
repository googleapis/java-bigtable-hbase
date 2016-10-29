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
package com.google.cloud.bigtable.grpc;

import java.util.List;

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
public class SimpleRow {
  public final static class SimpleColumn {
    private final String family;
    private final ByteString qualifier;
    private final long timestamp;
    private final ByteString value;
    private final List<String> labels;

    private SimpleColumn(String family, ByteString qualifier, long timestamp, ByteString value,
        List<String> labels) {
      this.family = family;
      this.qualifier = qualifier;
      this.timestamp = timestamp;
      this.value = value;
      this.labels = labels;
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
  }

  public static class FamilyColumnOrdering extends Ordering<SimpleColumn> {
    public static final FamilyColumnOrdering DEFAULT_ORDERING = new FamilyColumnOrdering();

    private FamilyColumnOrdering() {}

    @Override
    public int compare(SimpleColumn left, SimpleColumn right) {
      int result = left.family.compareTo(right.family);
      if (result == 0) {
        result = left.qualifier.toStringUtf8().compareTo(right.qualifier.toStringUtf8());
      }
      return result;
    }
  }

  public static final class Builder {
    private ByteString rowKey = null;
	private final ImmutableList.Builder<SimpleColumn> listBuilder;

    private Builder() {
      listBuilder = new ImmutableList.Builder<SimpleColumn>();
    }

    public static Builder newBuilder() {
      return new Builder();
    }

    public SimpleRow createRow() {
      return new SimpleRow(this);
    }

    public Builder setRowKey(ByteString rowKey) {
      if (rowKey == null) {
        throw new NullPointerException();
      }
      this.rowKey = rowKey;
      return this;
    }

    public ByteString getRowKey() {
      return rowKey;
    }

    public void addCell(String family, ByteString qualifier, long timestamp, ByteString value,
        List<String> labels) {
      listBuilder.add(new SimpleColumn(family, qualifier, timestamp, value, labels));
    }
  }

  private final ByteString rowKey;
  private final ImmutableList<SimpleColumn> columns;

  private SimpleRow(Builder builder) {
    this.rowKey = builder.getRowKey();
    this.columns = builder.listBuilder.build();
  }

  public ByteString getRowKey() {
    return rowKey;
  }

  public List<SimpleColumn> getList() {
    return columns;
  }
}