package com.google.cloud.hadoop.hbase;

import com.google.bigtable.v1.Row;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Abstract base class for Bigtable ResultScanner implementations that provides
 * a common next(int) implementation.
 */
public abstract class AbstractBigtableResultScanner implements ResultScanner<Row> {
  @Override
  public Row[] next(int count) throws IOException {
    ArrayList<Row> resultList = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      Row row = next();
      if (row == null) {
        break;
      }
      resultList.add(row);
    }
    return resultList.toArray(new Row[resultList.size()]);
  }
}
