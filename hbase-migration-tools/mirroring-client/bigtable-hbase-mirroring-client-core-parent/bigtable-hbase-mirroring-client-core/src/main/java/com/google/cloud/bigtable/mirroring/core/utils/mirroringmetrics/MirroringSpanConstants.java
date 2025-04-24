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

@InternalApi("For internal usage only")
public class MirroringSpanConstants {
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

    public String getString() {
      return string;
    }

    HBaseOperation(String name) {
      this.string = name;
    }
  }
}
