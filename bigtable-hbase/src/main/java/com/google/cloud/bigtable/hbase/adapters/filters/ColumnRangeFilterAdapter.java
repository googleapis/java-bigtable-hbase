/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Adapter for a single ColumnRangeFilter to a Cloud Bigtable RowFilter.
 */
public class ColumnRangeFilterAdapter implements TypedFilterAdapter<ColumnRangeFilter> {

  private static final String REQUIRE_SINGLE_FAMILY_MESSAGE =
      "Scan or Get operations using ColumnRangeFilter must "
          + "have a single family specified with #addFamily().";
  private static final FilterSupportStatus UNSUPPORTED_STATUS =
      FilterSupportStatus.newNotSupported(REQUIRE_SINGLE_FAMILY_MESSAGE);

  @Override
  public RowFilter adapt(FilterAdapterContext context, ColumnRangeFilter filter)
      throws IOException {
    byte[] familyName = getSingleFamily(context.getScan());
    ColumnRange.Builder rangeBuilder = ColumnRange.newBuilder();
    rangeBuilder.setFamilyName(Bytes.toString(familyName));

    ByteString startQualifier = ByteString.copyFrom(filter.getMinColumn());
    if (filter.getMinColumnInclusive()) {
      rangeBuilder.setStartQualifierClosed(startQualifier);
    } else {
      rangeBuilder.setStartQualifierOpen(startQualifier);
    }

    ByteString endQualifier = ByteString.copyFrom(filter.getMaxColumn());
    if (filter.getMaxColumnInclusive()) {
      rangeBuilder.setEndQualifierClosed(endQualifier);
    } else {
      rangeBuilder.setEndQualifierOpen(endQualifier);
    }
    return RowFilter.newBuilder()
        .setColumnRangeFilter(rangeBuilder)
        .build();
  }

  @Override
  public FilterSupportStatus isFilterSupported(
      FilterAdapterContext context,
      ColumnRangeFilter filter) {
    // We require a single column family to be specified:
    int familyCount = context.getScan().numFamilies();
    if (familyCount != 1) {
      return UNSUPPORTED_STATUS;
    }
    return FilterSupportStatus.SUPPORTED;
  }

  byte[] getSingleFamily(Scan scan) {
    return scan.getFamilies()[0];
  }
}
