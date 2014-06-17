package com.google.cloud.anviltop.hbase;


import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Methods to generate test data.
 */
public class DataGenerationHelper {

  protected byte[] randomData(String prefix) {
    return Bytes.toBytes(prefix + RandomStringUtils.randomAlphanumeric(8));
  }

  protected byte[][] randomData(String prefix, int count) {
    byte[][] result = new byte[count][];
    for (int i = 0; i < count; ++i) {
      result[i] = Bytes.toBytes(prefix + RandomStringUtils.randomAlphanumeric(8));
    }
    return result;
  }

  protected long[] sequentialTimestamps(int count) {
    return sequentialTimestamps(count, System.currentTimeMillis());
  }

  protected long[] sequentialTimestamps(int count, long firstValue) {
    assert count > 0;
    long[] timestamps = new long[count];
    timestamps[0] = firstValue;
    for (int i = 1; i < timestamps.length; ++i) {
      timestamps[i] = timestamps[0] + i;
    }
    return timestamps;
  }
}
