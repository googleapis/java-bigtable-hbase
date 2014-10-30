/*
 * Copyright (c) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData;
import com.google.bigtable.anviltop.AnviltopServices.GetRowResponse;
import com.google.cloud.anviltop.hbase.AnviltopConstants;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Adapt a GetRowResponse from Anviltop to an HBase Result
 */
public class GetRowResponseAdapter implements ResponseAdapter<GetRowResponse, Result> {
  protected final RowAdapter rowAdapter;

  public GetRowResponseAdapter(RowAdapter rowAdapter) {
   this.rowAdapter = rowAdapter;
  }

  /**
   * Transform an Anviltop server response to an HBase Result instance.
   *
   * @param response The Anviltop response to transform.
   */
  @Override
  public Result adaptResponse(GetRowResponse response) {
    List<Cell> cells = new ArrayList<Cell>();
    if (response.hasRow()) {
      return rowAdapter.adaptResponse(response.getRow());
    }
    return new Result();
  }
}
