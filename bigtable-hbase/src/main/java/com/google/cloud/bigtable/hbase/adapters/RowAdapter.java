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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.Row;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.cloud.bigtable.hbase.adapters.ResponseAdapterContext.Action;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Adapt a bigtable.v1.Row to an hbase client Result.
 */
public class RowAdapter implements ResponseAdapter<Row, Result> {
  
  private static final String WHILE_MATCH_FILTER_IN_LABEL_SUFFIX = "-in";
  private static final String WHILE_MATCH_FILTER_OUT_LABEL_SUFFIX = "-out";

  @Override
  public Result adaptResponse(Row response) {
    if (response == null) {
      return new Result();
    }

    List<org.apache.hadoop.hbase.Cell> hbaseCells = new ArrayList<>();
    byte[] rowKey = response.getKey().toByteArray();

    for (Family family : response.getFamiliesList()) {
      byte[] familyNameBytes = Bytes.toBytes(family.getName());

      for (Column column : family.getColumnsList()) {
        byte[] columnQualifier = column.getQualifier().toByteArray();

        for (Cell cell : column.getCellsList()) {
          long hbaseTimestamp =
              BigtableConstants.HBASE_TIMEUNIT.convert(
                  cell.getTimestampMicros(), BigtableConstants.BIGTABLE_TIMEUNIT);
          KeyValue keyValue = new KeyValue(
              rowKey,
              familyNameBytes,
              columnQualifier,
              hbaseTimestamp,
              cell.getValue().toByteArray());

          hbaseCells.add(keyValue);
        }
      }
    }

    Collections.sort(hbaseCells, KeyValue.COMPARATOR);

    return Result.create(hbaseCells);
  }

  /**
   * Similar to {@link #adaptResponse(Row)}, it handles {@link WhileMatchFilter} by checking for
   * mismatching "in" and "out" labels. If an "out" label is missing for a given "in" label of a
   * {@link WhileMatchFilter}, rescan is required after replacing the {@link WhileMatchFilter} with
   * a "None" or "Block All" filter.
   */
  @Override
  public Result adaptResponse(ResponseAdapterContext context, Row response) {
    if (response == null) {
      return new Result();
    }

    Map<String, Integer> inLabelMap = new HashMap<>();
    Map<String, Integer> outLabelMap = new HashMap<>();
    List<org.apache.hadoop.hbase.Cell> hbaseCells = new ArrayList<>();
    byte[] rowKey = response.getKey().toByteArray();

    for (Family family : response.getFamiliesList()) {
      byte[] familyNameBytes = Bytes.toBytes(family.getName());

      for (Column column : family.getColumnsList()) {
        byte[] columnQualifier = column.getQualifier().toByteArray();

        for (Cell cell : column.getCellsList()) {

          boolean areAllWhileMatchFilterLabels = true;
          for (String label : cell.getLabelsList()) {
            if (!context.containsWhileMatchFilterLabel(label)) {
              areAllWhileMatchFilterLabels = false;
              continue;
            }
            updateLabelCount(inLabelMap, label, WHILE_MATCH_FILTER_IN_LABEL_SUFFIX);
            updateLabelCount(outLabelMap, label, WHILE_MATCH_FILTER_OUT_LABEL_SUFFIX);
          }

          // A cell has {@link WhileMatchFilter} labels only is for internal use, do not return
          // it.
          if (cell.getLabelsCount() > 0 && areAllWhileMatchFilterLabels) {
            continue;
          }

          long hbaseTimestamp =
              BigtableConstants.HBASE_TIMEUNIT.convert(
                  cell.getTimestampMicros(), BigtableConstants.BIGTABLE_TIMEUNIT);
          KeyValue keyValue = new KeyValue(
              rowKey,
              familyNameBytes,
              columnQualifier,
              hbaseTimestamp,
              cell.getValue().toByteArray());

          hbaseCells.add(keyValue);
        }
      }
    }

    // check if there are non-matching labels.
    for (String whileMatchFilterId : inLabelMap.keySet()) {
      int inCount = inLabelMap.get(whileMatchFilterId);
      Integer outCount = outLabelMap.get(whileMatchFilterId);
      if (outCount == null || inCount != outCount.intValue()) {
        // TODO(kevinsi4508): Pass {@code whileMatchFilterId} in the context. It is needed in
        // rescan.
        context.setAction(Action.RESCAN);
        return new Result();
      }
    }

    // Skips an empty row.
    if (hbaseCells.isEmpty()) {
      context.setAction(Action.SKIP);
      return new Result();
    }

    Collections.sort(hbaseCells, KeyValue.COMPARATOR);
    context.setAction(Action.NEXT);
    return Result.create(hbaseCells);
  }

  /**
   * Updates the label count for a {@link WhileMatchFilter} label.
   * 
   * @param labelMap contains the occurrence of each label. 
   * @param label {@link WhileMatchFilter} label with a suffix
   * @param suffix {@link WhileMatchFilter} label suffix
   */
  private static void updateLabelCount(Map<String, Integer> labelMap, String label, String suffix) {
    int suffixIndex = label.lastIndexOf(suffix);
    if (suffixIndex == -1) {
      return;
    }

    String whileMatchFilterId = label.substring(0, suffixIndex);
    Integer count = labelMap.get(whileMatchFilterId);
    if (count == null) {
      count = new Integer(0);
    }
    labelMap.put(whileMatchFilterId, ++count);
  }
}
