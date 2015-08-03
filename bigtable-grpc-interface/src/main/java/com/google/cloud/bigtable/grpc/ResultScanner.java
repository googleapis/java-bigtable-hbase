/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.grpc;


import java.io.IOException;

/**
 * A scanner of Bigtable rows.
 * @param <T> The type of Rows this scanner will iterate over. Expected Bigtable Row objects.
 */
public interface ResultScanner<T> {
  /**
   * Read the next row and block until a row is available. Will return null on end-of-stream.
   */
  T next() throws IOException;

  /**
   * Read the next N rows where N &lt;= count. Will block until count are available or end-of-stream is
   * reached.
   * @param count The number of rows to read.
   */
  T[] next(int count) throws IOException;

  /**
   * Close the scanner and release any resources allocated for it.
   */
  void close() throws IOException;
}
