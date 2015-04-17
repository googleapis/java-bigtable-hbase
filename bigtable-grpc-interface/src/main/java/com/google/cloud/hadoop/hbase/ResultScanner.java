package com.google.cloud.hadoop.hbase;


import java.io.IOException;

/**
 * A scanner of Bigtable rows.
 * @param <T> The type of Rows this scanner will iterate over. Expected Bigtable Row objects.
 */
public interface ResultScanner<T> {
  /**
   * Read the next row & block until a row is available. Will return null on end-of-stream.
   */
  T next() throws IOException;

  /**
   * Read the next N rows where N <= count. Will block until count are available or end-of-stream is
   * reached.
   * @param count The number of rows to read.
   */
  T[] next(int count) throws IOException;

  /**
   * Close the scanner and release any resources allocated for it.
   */
  void close() throws IOException;
}
