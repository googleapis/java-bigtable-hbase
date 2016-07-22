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

/**
 * A factory for creating ResultScanners that can be used to scan over Rows for a
 * given ReadRowsRequest.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public interface BigtableResultScannerFactory<RequestT, ResponseT> {

  /**
   * Create a scanner for the given request.
   *
   * @param request a RequestT object.
   * @return a {@link com.google.cloud.bigtable.grpc.scanner.ResultScanner} object.
   */
  ResultScanner<ResponseT> createScanner(RequestT request);
}
