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
package com.google.cloud.bigtable.mapreduce.validation;

import com.google.cloud.bigtable.mapreduce.validation.SyncTableValidationOnlyJob.SyncMapper;
import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/** helper class for formatting validation records for output */
public class RecordFormatter {

  public static final String REASON_CODE_STR = "reason_code";

  /**
   * formatting for range mismatch
   * @param reasonCode
   * @param startRow
   * @param stopRow
   * @param sourceHash
   * @param targetHash
   * @return
   */
  public static String formatRangeMismatch(
      SyncMapper.Counter reasonCode,
      String startRow,
      String stopRow,
      String sourceHash,
      String targetHash) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty(REASON_CODE_STR, reasonCode.name());
    jsonObj.addProperty("range_start_row", startRow);
    jsonObj.addProperty("range_stop_row", stopRow);
    jsonObj.addProperty("range_source_hash", sourceHash);
    jsonObj.addProperty("range_target_hash", targetHash);
    return jsonObj.toString();
  }

  /**
   * single cell validation formatter (ie. missing in source)
   *
   * @param reasonCode
   * @param cell
   * @return
   */
  public static String format(SyncMapper.Counter reasonCode, Cell cell) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty(REASON_CODE_STR, reasonCode.name());
    jsonObj.add("cell", formatCellToJsonObj(cell));
    return jsonObj.toString();
  }

  /**
   * source/target cell validation formatter (ie. byte mismatch)
   *
   * @param reasonCode
   * @param sourceCell
   * @param targetCell
   * @return
   */
  public static String format(SyncMapper.Counter reasonCode, Cell sourceCell, Cell targetCell) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty(REASON_CODE_STR, reasonCode.name());
    jsonObj.add("source_cell", formatCellToJsonObj(sourceCell));
    jsonObj.add("target_cell", formatCellToJsonObj(targetCell));
    return jsonObj.toString();
  }

  /**
   * convert cell to json for standard representation
   *
   * @param cell
   * @return
   */
  public static JsonObject formatCellToJsonObj(Cell cell) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty(
        "row", Bytes.toStringBinary(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    jsonObj.addProperty(
        "family",
        Bytes.toStringBinary(
            cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    jsonObj.addProperty(
        "column",
        Bytes.toStringBinary(
            cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
    jsonObj.addProperty("timestamp", cell.getTimestamp());
    jsonObj.addProperty("vlen", cell.getValueLength());
    jsonObj.addProperty("type", KeyValue.Type.codeToType(cell.getTypeByte()).name());

    return jsonObj;
  }
}
