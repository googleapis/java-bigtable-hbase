package com.google.cloud.bigtable.hbase.adapters.filters;

import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;

/**
 * An interface that classes should implement if they're able to contribute
 * meaningfully to collecting unsupported status objects from child filters.
 */
public interface UnsupportedStatusCollector<S extends Filter> {

  /**
   * Collect FilterSupportStatuses from the filter Filter and all subfilters.
   */
  void collectUnsupportedStatuses(
      FilterAdapterContext context,
      S filter,
      List<FilterSupportStatus> unsupportedStatuses);
}
