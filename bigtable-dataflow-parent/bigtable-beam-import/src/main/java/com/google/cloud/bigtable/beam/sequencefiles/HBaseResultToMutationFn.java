/*
 * Copyright 2017 Google LLC
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

import com.google.cloud.bigtable.beam.hbasesnapshots.ImportJobFromHbaseSnapshot.ImportOptions.CustomPipelineOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

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
 * A {@link DoFn} function that converts a {@link Result} in the pipeline input to a {@link
 * Mutation} for output.
 */

public class HBaseResultToMutationFn extends DoFn<KV<ImmutableBytesWritable, Result>, Mutation> {
  private static Logger logger = LoggerFactory.getLogger(HBaseResultToMutationFn.class);

  private static final long serialVersionUID = 1L;

  private static final int MAX_CELLS = 100_000 - 1;

  private static final int MAX_VALUE = 999999999; //999,999,999

  private static final Predicate<Cell> IS_DELETE_MARKER_FILTER =
          new Predicate<Cell>() {
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

    CustomPipelineOptions customOpts = context.getPipelineOptions().as(CustomPipelineOptions.class);

    if (customOpts.getTenantId() != null) {
      stringPrefixProcess(context, customOpts);
      //integerPrefixProcess(context, customOpts);
    }
    else {
      originalProcess(context);
    }

    //runningTotalSizeProcess(context, customOpts);
    //singleTableReverseProcess(context, customOpts);
  }

  /**
   Original, vanilla HBaseResultToMutationFn - doesn't perform any transformations on import data.
   **/
  private void originalProcess(ProcessContext context) throws IOException {
    logger.info("Starting HBaseResultToMutationFn in origin mode.");

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

    while (cellIt.hasNext()) {
      Put originalPut = new Put(kv.getKey().get());
      for (int i = 0; i < MAX_CELLS && cellIt.hasNext(); i++) {
        Cell originalCell = cellIt.next();
        originalPut.add(originalCell);
      }
      context.output(originalPut);
    }
  }

  /**
   Original, vanilla HBaseResultToMutationFn - doesn't perform any transformations on import data.
   **/
  private void runningTotalSizeProcess(ProcessContext context) throws IOException {

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

    int totalByteSize = 0;

    while (cellIt.hasNext()) {
      Put originalPut = new Put(kv.getKey().get());
      for (int i = 0; i < MAX_CELLS && cellIt.hasNext(); i++) {
        Cell originalCell = cellIt.next();

        totalByteSize = totalByteSize + originalCell.getRowArray().length;

        originalPut.add(originalCell);
      }
      context.output(originalPut);
    }
  }

  /**
   Modified HBaseResultToMutationFn - adds a reader legible string prefix to each incoming row key.
   **/
  private void stringPrefixProcess(ProcessContext context, CustomPipelineOptions customOpts) throws IOException {
    logger.info("Starting HBaseResultToMutationFn in string_prefix mode.");

    KV<ImmutableBytesWritable, Result> kv = context.element();
    byte[] stringPrefixedRowKey = stringPrefixRowKeyTransform(kv.getKey(), customOpts);

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

    while (cellIt.hasNext()) {
      Put stringPrefixedPut = new Put(stringPrefixedRowKey);
      for (int i = 0; i < MAX_CELLS && cellIt.hasNext(); i++) { // divide max cells by two when performing reverse scans
        Cell originalCell = cellIt.next();

        Cell prefixedCell = CellUtil.createCell(
                stringPrefixedRowKey,
                CellUtil.cloneFamily(originalCell),
                CellUtil.cloneQualifier(originalCell),
                originalCell.getTimestamp(),
                originalCell.getTypeByte(),
                CellUtil.cloneValue(originalCell));
        stringPrefixedPut.add(prefixedCell);
      }
      context.output(stringPrefixedPut);
    }
  }

  /**
   Modified HBaseResultToMutationFn - adds a reversed byte array integer prefix to each incoming row key. may not be
   human legible in Bigtable (i.e. shows up as symbols)
   **/
  private void integerPrefixProcess(ProcessContext context, CustomPipelineOptions customOpts) throws IOException {
    logger.info("Starting HBaseResultToMutationFn in integer_prefix mode.");

    KV<ImmutableBytesWritable, Result> kv = context.element();

    Integer tenantId = Integer.parseInt(customOpts.getTenantId().toString());
    byte[] integerPrefixedRowKey = integerPrefixRowKeyTransform(kv.getKey(), customOpts);

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

    while (cellIt.hasNext()) {
      Put integerPrefixedPut = new Put(integerPrefixedRowKey);
      for (int i = 0; i < MAX_CELLS && cellIt.hasNext(); i++) { // divide max cells by two when performing reverse scans
        Cell originalCell = cellIt.next();

        Cell prefixedCell = CellUtil.createCell(
                integerPrefixedRowKey,
                CellUtil.cloneFamily(originalCell),
                CellUtil.cloneQualifier(originalCell),
                originalCell.getTimestamp(),
                originalCell.getTypeByte(),
                CellUtil.cloneValue(originalCell));
        integerPrefixedPut.add(prefixedCell);
      }
      context.output(integerPrefixedPut);
    }
  }

  /**
   Modified HBaseResultToMutationFn - takes a row key with a timestamp, duplicates and reverses the timestamp
   **/
  private void singleTableReverseProcess(ProcessContext context, CustomPipelineOptions customOpts) throws IOException {
    logger.info("Starting HBaseResultToMutationFn in single_table_reverse mode.");

    KV<ImmutableBytesWritable, Result> kv = context.element();
    byte[] stringPrefixedRowKey = stringPrefixRowKeyTransform(kv.getKey(), customOpts);
    byte[] reversedRowKey = reverseRowKey(stringPrefixedRowKey, customOpts);

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

    while (cellIt.hasNext()) {
      Put stringPrefixedPut = new Put(stringPrefixedRowKey);
      Put reversedPut = new Put(reversedRowKey);
      for (int i = 0; i < MAX_CELLS / 2 && cellIt.hasNext(); i++) { // divide max cells by two when performing reverse scans
        Cell originalCell = cellIt.next();

        // original
        Cell stringPrefixedCell = CellUtil.createCell(
                stringPrefixedRowKey,
                CellUtil.cloneFamily(originalCell),
                CellUtil.cloneQualifier(originalCell),
                originalCell.getTimestamp(),
                originalCell.getTypeByte(),
                CellUtil.cloneValue(originalCell));
        stringPrefixedPut.add(stringPrefixedCell);

        // reversed
        Cell reversedCell = CellUtil.createCell(
                reversedRowKey,
                CellUtil.cloneFamily(originalCell),
                CellUtil.cloneQualifier(originalCell),
                originalCell.getTimestamp(),
                originalCell.getTypeByte(),
                CellUtil.cloneValue(originalCell));
        reversedPut.add(reversedCell);
      }

      context.output(stringPrefixedPut);
      context.output(reversedPut);
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
                            COLUMN_FAMILY_EXTRACTOR)
                    .asMap();
    Map<String, Collection<Cell>> deleteMarkersByFamilyMap =
            Multimaps.index(Iterables.filter(cells, IS_DELETE_MARKER_FILTER), COLUMN_FAMILY_EXTRACTOR)
                    .asMap();
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
              Predicates.not(
                      Predicates.or(
                              Lists.newArrayList(
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

  /**
   * Transform row key with tenant id expected as string
   * @param currentKey
   * @param customOpts
   * @return
   */
  protected byte[] stringPrefixRowKeyTransform(ImmutableBytesWritable currentKey,
                                               CustomPipelineOptions customOpts) {

    String tenantId = customOpts.getTenantId().toString();
    String delimiter = customOpts.getDelimiter().toString();
    tenantId = tenantId.concat(delimiter);

    return ByteBuffer.allocate(currentKey.get().length + tenantId.getBytes().length)
            .put(ByteBuffer.allocate(tenantId.getBytes().length).put(tenantId.getBytes()).array())
            .put(currentKey.get())
            .array();
  }

  /**
   * Transform row key with tenant id expected as integer
   * @param currentKey
   * @param customOpts
   * @return
   */
  protected byte[] integerPrefixRowKeyTransform(ImmutableBytesWritable currentKey,
                                                CustomPipelineOptions customOpts) {
    // TODO - check rowKeyNamespace.length required
    // TODO: this is returning indecipherable symbols for integer prefix in BigTable results

    Integer tenantId = Integer.valueOf(customOpts.getTenantId().toString());

    return ByteBuffer.allocate(currentKey.get().length + Integer.BYTES)
            .put(ByteBuffer.allocate(Integer.BYTES).putInt(Integer.reverseBytes(tenantId)).array())
            .put(currentKey.get())
            .array();
  }


  /**
   * Reverse the last value within a row key
   * @param key
   * @param customOpts
   * @return
   */
  protected byte[] reverseRowKey(byte[] key, CustomPipelineOptions customOpts) {

    String delimiter = customOpts.getDelimiter().toString();
    String[] originalKeyParts = new String(key, StandardCharsets.UTF_8).split(delimiter);
    Integer reversedVal = MAX_VALUE - Integer.parseInt(originalKeyParts[originalKeyParts.length-1]);

    // compile new key
    String newKey = "";
    for (int i = 0; i < originalKeyParts.length - 1; i++) {
      if (i == 1) {
        newKey = newKey.concat("rev" + delimiter);
      }
      newKey = newKey.concat(originalKeyParts[i] + delimiter);
    }                                                             // tenantId#msg11#fromNardos#toAdbul#982736578
    newKey = newKey.concat(reversedVal.toString());               // tenantId#rev#msg11#fromNardos#toAbdul#17263421

//    logger.warn("reversedVal:" + reversedVal);
//    logger.warn("newKey: " + newKey);

    return ByteBuffer.allocate(newKey.getBytes().length)
            .put(newKey.getBytes())
            .array();
  }
}
