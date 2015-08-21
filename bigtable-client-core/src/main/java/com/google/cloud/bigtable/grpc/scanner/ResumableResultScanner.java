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
package com.google.cloud.bigtable.grpc.scanner;


import com.google.bigtable.v1.ReadRowsRequest;

/**
 * A resumable scanner of Bigtable rows.
 * @param <T> The type of Rows this scanner will iterate over. Expected Bigtable Row objects.
 */
public interface ResumableResultScanner<T> extends ResultScanner<T> {
  /**
   * Stop and start a new scan with {@code newRequest}. The subclass should maintain the last row
   * read if the starting row matters when resuming the scan.
   *
   * @param newRequest the quest to be used when resuming a new scan
   * @param skipLastRow if should skip the last row read and start scanning from the next row
   */
  void resume(ReadRowsRequest.Builder newRequest, boolean skipLastRow);
}
