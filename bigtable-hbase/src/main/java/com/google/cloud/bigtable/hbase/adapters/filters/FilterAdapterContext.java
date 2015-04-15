package com.google.cloud.bigtable.hbase.adapters.filters;

import org.apache.hadoop.hbase.client.Scan;

/**
 * Context for the currently executing filter adapter.
 */
public class FilterAdapterContext {
  private final Scan scan;

  public FilterAdapterContext(Scan scan) {
    this.scan = scan;
  }

  Scan getScan() {
    return scan;
  }

  // TODO: Consider adding a way for a filter to modify results that come
  // back on the client-side (e.g., addPostProccessingStep(...)
}

