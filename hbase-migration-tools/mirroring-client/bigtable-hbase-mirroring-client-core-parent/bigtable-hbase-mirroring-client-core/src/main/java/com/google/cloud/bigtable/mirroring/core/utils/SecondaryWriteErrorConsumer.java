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
import com.google.cloud.bigtable.mirroring.core.utils.faillog.FailedMutationLogger;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringSpanConstants.HBaseOperation;
import java.util.List;
import org.apache.hadoop.hbase.client.Row;

/**
 * Implementations of this interface consume mutations ({@link
 * org.apache.hadoop.hbase.client.Mutation} and {@link org.apache.hadoop.hbase.client.RowMutations})
 * that succeeded on primary database but have failed on the secondary.
 *
 * <p>Default implementation ({@link DefaultSecondaryWriteErrorConsumer}) forwards those writes to
 * {@link FailedMutationLogger} (which, by default, writes them to on-disk log).
 */
@InternalApi("For internal usage only")
public interface SecondaryWriteErrorConsumer {
  void consume(HBaseOperation operation, Row row, Throwable cause);

  void consume(HBaseOperation operation, List<? extends Row> operations, Throwable cause);

  interface Factory {
    SecondaryWriteErrorConsumer create(FailedMutationLogger failedMutationLogger) throws Throwable;
  }
}
