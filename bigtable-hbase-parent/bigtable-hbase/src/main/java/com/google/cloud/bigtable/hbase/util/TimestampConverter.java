package com.google.cloud.bigtable.hbase.util;


import com.google.cloud.bigtable.hbase.BigtableConstants;
import org.apache.hadoop.hbase.HConstants;

public class TimestampConverter {
  private static final long STEP = BigtableConstants.BIGTABLE_TIMEUNIT
      .convert(1, BigtableConstants.HBASE_TIMEUNIT);

  /**
   * Maximum timestamp (in usecs) that bigtable can handle while preserving lossless conversion to hbase timestamps in
   * ms)
   */
  private static final long BIGTABLE_MAX_TIMESTAMP = Long.MAX_VALUE - (Long.MAX_VALUE % STEP);

  /**
   * Maximum timestamp that hbase can send to bigtable in ms. This limitation exists because bigtable operates on usecs,
   * while hbase operates on ms.
   */
  private static final long HBASE_EFFECTIVE_MAX_TIMESTAMP = BigtableConstants.HBASE_TIMEUNIT
      .convert(BIGTABLE_MAX_TIMESTAMP, BigtableConstants.BIGTABLE_TIMEUNIT);

  public static long hbase2bigtable(long timestamp) {
    if (timestamp < HBASE_EFFECTIVE_MAX_TIMESTAMP) {
      return BigtableConstants.BIGTABLE_TIMEUNIT.convert(timestamp, BigtableConstants.HBASE_TIMEUNIT);
    } else {
      return BIGTABLE_MAX_TIMESTAMP;
    }
  }

  public static long bigtable2hbase(long timestamp) {
    if (timestamp >= BIGTABLE_MAX_TIMESTAMP) {
      return HConstants.LATEST_TIMESTAMP;
    } else {
      return BigtableConstants.HBASE_TIMEUNIT.convert(timestamp, BigtableConstants.BIGTABLE_TIMEUNIT);
    }
  }
}
