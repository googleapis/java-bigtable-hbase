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

package com.google.cloud.bigtable.beam.validation;

import static com.google.cloud.bigtable.beam.validation.SyncTableUtils.createConfiguration;

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import java.io.IOException;
import java.io.Serializable;

/** Factory to create a TableHashWrapper. */
@InternalApi
public class TableHashWrapperFactory implements Serializable {

  private static final long serialVersionUID = 265433454L;

  public TableHashWrapper getTableHash(String projectId, String sourceHashDir) throws IOException {
    return TableHashWrapperImpl.create(
        createConfiguration(projectId, sourceHashDir), sourceHashDir);
  }
}
