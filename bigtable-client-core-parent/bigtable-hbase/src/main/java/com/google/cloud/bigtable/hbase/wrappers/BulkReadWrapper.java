/*
 * Copyright 2020 Google LLC
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
package com.google.cloud.bigtable.hbase.wrappers;

import com.google.api.core.ApiFuture;
import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.protobuf.ByteString;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.hadoop.hbase.client.Result;

/**
 * Interface to read multiple rows in batched mode from a single table.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public interface BulkReadWrapper extends AutoCloseable {

  /**
   * Adds a {@code rowKey} to a batch read row request with an optional {@link Filters.Filter}. The
   * returned future will be resolved when the batch response is received.
   */
  ApiFuture<Result> add(ByteString rowKey, @Nullable Filters.Filter filter);

  /**
   * Sends all remaining requests to the server. This method does not wait for the method to
   * complete.
   */
  void flush();

  /** Closes the batch and prevents new keys addition. */
  void close() throws IOException;
}
