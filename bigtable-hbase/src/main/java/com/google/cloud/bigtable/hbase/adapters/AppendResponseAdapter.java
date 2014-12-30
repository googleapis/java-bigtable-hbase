package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopServiceMessages.AppendRowRequest;
import com.google.bigtable.anviltop.AnviltopServiceMessages.AppendRowResponse;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapt an AppendRowResponse from Anviltop to an HBase Result
 */
public class AppendResponseAdapter
    implements ResponseAdapter<AppendRowResponse, Result> {
  protected final RowAdapter rowAdapter;

  public AppendResponseAdapter(RowAdapter rowAdapter) {
    this.rowAdapter = rowAdapter;
  }

  @Override
  public Result adaptResponse(AppendRowResponse response) {
    List<Cell> cells = new ArrayList<Cell>();
    if (response.hasRow()) {
      return rowAdapter.adaptResponse(response.getRow());
    }
    return null;
  }
}
