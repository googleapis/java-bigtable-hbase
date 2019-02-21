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

import com.google.api.client.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} function that converts a {@link Result} in the pipeline input to a
 * {@link Mutation} for output.
 */
class HBaseResultToMutationFn
    extends DoFn<KV<ImmutableBytesWritable, Result>, Mutation> {
  private static Logger logger = LoggerFactory.getLogger(HBaseResultToMutationFn.class);

  private static final long serialVersionUID = 1L;

  private static final int MAX_CELLS = 100_000 - 1;

  private static final Predicate<Cell> IS_DELETE_MARKER_FILTER =  new Predicate<Cell>() {
    @Override
    public boolean apply(Cell cell) {
      return CellUtil.isDelete(cell);
    }
  };

  private static final Function<Cell, String> COLUMN_FAMILY_EXTRACTOR =
      new Function<Cell, String>() {
        @Override
        public String apply(Cell cell) {
          return Bytes.toString(CellUtil.cloneFamily(cell));
        }
      };

  private static final DataCellPredicateFactory DATA_CELL_PREDICATE_FACTORY =
      new DataCellPredicateFactory();

  private transient boolean isEmptyRowWarned;

  @VisibleForTesting
  static void setLogger(Logger log) {
    logger = log;
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    KV<ImmutableBytesWritable, Result> kv = context.element();
    List<Cell> cells = checkEmptyRow(kv);
    if (cells.isEmpty()) {
      return;
    }

    // Preprocess delete markers
    if (hasDeleteMarkers(cells)) {
      cells = preprocessDeleteMarkers(cells);
    }

    // Split the row into multiple puts if it exceeds the maximum mutation limit
    Iterator<Cell> cellIt = cells.iterator();

    while(cellIt.hasNext()) {
      Put put = new Put(kv.getKey().get());

      for(int i=0; i < MAX_CELLS && cellIt.hasNext(); i++) {
        put.add(cellIt.next());
      }

      context.output(put);
    }
  }

  private boolean hasDeleteMarkers(List<Cell> cells) {
    for (Cell cell : cells) {
      if (CellUtil.isDelete(cell)) {
        return true;
      }
    }
    return false;
  }

  // Process
  private List<Cell> preprocessDeleteMarkers(List<Cell> cells) {
    List<Cell> resultCells = Lists.newArrayList();

    // Group cells by column family, since DeleteMarkers do not apply across families.
    Map<String, Collection<Cell>> dataCellsByFamilyMap =
        Multimaps.index(
            Iterables.filter(cells, Predicates.not(IS_DELETE_MARKER_FILTER)),
            COLUMN_FAMILY_EXTRACTOR).asMap();
    Map<String, Collection<Cell>> deleteMarkersByFamilyMap =
        Multimaps.index(
            Iterables.filter(cells, IS_DELETE_MARKER_FILTER), COLUMN_FAMILY_EXTRACTOR).asMap();
    for (Map.Entry<String, Collection<Cell>> e : dataCellsByFamilyMap.entrySet()) {
      processOneColumnFamily(resultCells, e.getValue(), deleteMarkersByFamilyMap.get(e.getKey()));
    }
    return resultCells;
  }

  private void processOneColumnFamily(
      List<Cell> resultCells, Collection<Cell> dataCells, Collection<Cell> deleteMarkers) {
    if (deleteMarkers == null) {
      // No markers for this column family
      resultCells.addAll(dataCells);
    } else {
      // Build a filter for live data cells that should be sent to bigtable.
      // These are cells not marked by any delete markers in this row/family.
      Predicate<Cell> liveDataCellPredicate =
          Predicates.not(Predicates.or(Lists.newArrayList(
              Iterables.transform(deleteMarkers, DATA_CELL_PREDICATE_FACTORY))));
      for (Cell cell : dataCells) {
        if (liveDataCellPredicate.apply(cell)) {
          resultCells.add(cell);
        }
      }
    }
  }

  // Warns about empty row on first occurrence only and replaces a null array with 0-length one.
  private List<Cell> checkEmptyRow(KV<ImmutableBytesWritable, Result> kv) {
    List<Cell> cells = kv.getValue().listCells();
    if (cells == null) {
      cells = Collections.emptyList();
    }
    if (!isEmptyRowWarned && cells.isEmpty()) {
      logger.warn("Encountered empty row. Was input file serialized by HBase 0.94?");
      isEmptyRowWarned = true;
    }
    return cells;
  }
}
