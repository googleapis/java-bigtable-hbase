/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics;

import com.google.api.core.InternalApi;
import io.opencensus.stats.Measure.MeasureLong;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;

@InternalApi("For internal usage only")
public class MirroringSpanConstants {
  public static final MeasureLong PRIMARY_LATENCY =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/primary/latency",
          "Distribution of operation latency on primary database",
          "ms");

  public static final MeasureLong SECONDARY_LATENCY =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/secondary/latency",
          "Distribution of operation latency on secondary database",
          "ms");

  public static final MeasureLong MIRRORING_LATENCY =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/mirroring/latency",
          "Distribution of operation latency on secondary database.",
          "ms");

  public static final MeasureLong PRIMARY_ERRORS =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/primary/error_rate",
          "Count of errors on primary database.",
          "1");

  public static final MeasureLong SECONDARY_ERRORS =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/secondary/error_rate",
          "Count of errors on secondary database.",
          "1");

  public static final MeasureLong READ_MATCHES =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/read_verification/matches",
          "Count of successfully verified reads.",
          "1");

  public static final MeasureLong READ_MISMATCHES =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/read_verification/mismatches",
          "Count of read mismatches detected.",
          "1");

  public static final MeasureLong SECONDARY_WRITE_ERRORS =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/secondary/write_error_rate",
          "Count of write errors on secondary database.",
          "1");

  public static final MeasureLong FLOW_CONTROL_LATENCY =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/flow_control_latency",
          "Distribution of latency of acquiring flow controller resources.",
          "ms");

  public static final MeasureLong SECONDARY_WRITE_ERROR_HANDLER_LATENCY =
      MeasureLong.create(
          "com/google/cloud/bigtable/mirroring/secondary_write_error_handler_latency",
          "Distribution of secondary write error handling latency.",
          "ms");

  public static TagKey OPERATION_KEY = TagKey.create("operation");

  public enum HBaseOperation {
    GET("get"),
    GET_LIST("getList"),
    EXISTS("exists"),
    EXISTS_ALL("existsAll"),
    PUT("put"),
    PUT_LIST("putList"),
    DELETE("delete"),
    DELETE_LIST("deleteList"),
    NEXT("next"),
    NEXT_MULTIPLE("nextMultiple"),
    CHECK_AND_PUT("checkAndPut"),
    CHECK_AND_DELETE("checkAndDelete"),
    CHECK_AND_MUTATE("checkAndMutate"),
    MUTATE_ROW("mutateRow"),
    APPEND("append"),
    INCREMENT("increment"),
    GET_SCANNER("getScanner"),
    BATCH("batch"),
    BATCH_CALLBACK("batchCallback"),
    TABLE_CLOSE("close"),
    GET_TABLE("getTable"),
    BUFFERED_MUTATOR_FLUSH("flush"),
    BUFFERED_MUTATOR_MUTATE("mutate"),
    BUFFERED_MUTATOR_MUTATE_LIST("mutateList"),
    MIRRORING_CONNECTION_CLOSE("MirroringConnection.close"),
    MIRRORING_CONNECTION_ABORT("MirroringConnection.abort"),
    BUFFERED_MUTATOR_CLOSE("BufferedMutator.close");

    private final String string;
    private final TagValue tagValue;

    public String getString() {
      return string;
    }

    public TagValue getTagValue() {
      return tagValue;
    }

    HBaseOperation(String name) {
      this.string = name;
      this.tagValue = TagValue.create(name);
    }
  }
}
