package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopServices.IncrementRowResponse;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapt a IncrementRowResponse from Anviltop to an HBase Result
 */
public class IncrementRowResponseAdapter implements ResponseAdapter<IncrementRowResponse, Result> {
  protected final RowAdapter rowAdapter;

  public IncrementRowResponseAdapter(RowAdapter rowAdapter) {
    this.rowAdapter = rowAdapter;
  }

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
