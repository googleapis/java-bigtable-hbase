/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.conf;

import com.google.api.core.InternalApi;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

/**
 * A lightweight key representing a snapshot and its target table, used to avoid serializing full
 * configuration details for every row.
 */
@InternalApi("For internal usage only")
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SnapshotKey implements Serializable {
  @SchemaCreate
  public static SnapshotKey create(String snapshotName, String tableName) {
    return new AutoValue_SnapshotKey(snapshotName, tableName);
  }

  public abstract String getSnapshotName();

  public abstract String getTableName();
}
