package com.google.cloud.bigtable.hbase.adapters.filters;

import com.google.bigtable.v1.RowFilter;

import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * An adapter that can adapt an HBase Filter instance into a Bigtable RowFilter.
 */
public interface TypedFilterAdapter<S extends Filter> {

  /**
   * Adapt the given filter. Implementers of this method should assume that
   * isFilterSupported has already been called with a result indicating it
   * is in fact supproted.
   */
  RowFilter adapt(FilterAdapterContext context, S filter) throws IOException;

  /**
   * Determine if the given filter can be adapted to a Bigtable RowFilter.
   */
  FilterSupportStatus isFilterSupported(FilterAdapterContext context, S filter);
}
