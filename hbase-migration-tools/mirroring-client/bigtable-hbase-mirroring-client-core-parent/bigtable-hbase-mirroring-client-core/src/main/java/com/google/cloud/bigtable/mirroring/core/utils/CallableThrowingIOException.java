/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.core.utils;

import com.google.api.core.InternalApi;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 * A specialization of {@link Callable} that tightens list of thrown exceptions to {@link
 * IOException}, which is the only exceptions thrown by some of operations in HBase API ({@link
 * Table#put(Put)}). Facilitates error handling when such operations are used as callbacks.
 */
@InternalApi("For internal usage only")
public interface CallableThrowingIOException<T>
    extends CallableThrowingIOAndInterruptedException<T> {
  T call() throws IOException;
}
