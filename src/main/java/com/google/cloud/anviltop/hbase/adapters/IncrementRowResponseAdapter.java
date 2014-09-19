package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData;
import com.google.bigtable.anviltop.AnviltopServices.IncrementRowResponse;
import com.google.cloud.anviltop.hbase.AnviltopConstants;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Adapt a IncrementRowResponse from Anviltop to an HBase Result
 */
public class IncrementRowResponseAdapter implements ResponseAdapter<IncrementRowResponse, Result> {
  protected final RowAdapter rowAdapter = new RowAdapter();

  /**
   * Transform an Anviltop server response to an HBase Result instance.
   *
   * @param response The Anviltop response to transform.
   */
  @Override
  public Result adaptResponse(IncrementRowResponse response) {
    List<Cell> cells = new ArrayList<Cell>();
    if (response.hasRow()) {
      return rowAdapter.adaptResponse(response.getRow());
    }
    return new Result();
  }
}
