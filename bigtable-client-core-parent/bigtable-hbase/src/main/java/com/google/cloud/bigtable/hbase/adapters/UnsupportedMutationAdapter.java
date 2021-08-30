/*
 * Copyright 2015 Google LLC
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
package com.google.cloud.bigtable.hbase.adapters;

import com.google.api.core.InternalApi;
import org.apache.hadoop.hbase.client.Mutation;

/**
 * An adapter that throws an Unsupported exception when its adapt method is invoked.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class UnsupportedMutationAdapter<T extends Mutation> extends MutationAdapter<T> {

  private final String operationDescription;

  /**
   * Constructor for UnsupportedOperationAdapter.
   *
   * @param operationDescription a {@link java.lang.String} object.
   */
  public UnsupportedMutationAdapter(String operationDescription) {
    this.operationDescription = operationDescription;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Adapt a single HBase Operation to a single Bigtable generated message.
   */
  @Override
  public void adapt(T operation, com.google.cloud.bigtable.data.v2.models.MutationApi<?> mutation) {
    throw new UnsupportedOperationException(
        String.format("The %s operation is unsupported.", operationDescription));
  }
}
