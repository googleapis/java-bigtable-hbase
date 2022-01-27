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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.timestamper;

import com.google.api.core.InternalApi;
import java.util.List;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;

/**
 * Timestamper implementations are responsible for adding (or not) timestamps to {@link Put}s before
 * they are sent to underlying databases.
 */
@InternalApi("For internal use only")
public interface Timestamper {

  <T extends Row> List<T> fillTimestamp(List<T> list);

  RowMutations fillTimestamp(RowMutations rowMutations);

  Put fillTimestamp(Put put);

  Mutation fillTimestamp(Mutation mutation);
}
