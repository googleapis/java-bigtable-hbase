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
package com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol;

import com.google.api.core.InternalApi;
import org.apache.hadoop.hbase.client.Result;

@InternalApi("For internal usage only")
public class RequestResourcesDescription {
  public final int numberOfResults;

  private RequestResourcesDescription(int numberOfResults) {
    this.numberOfResults = numberOfResults;
  }

  public RequestResourcesDescription(boolean bool) {
    this(1);
  }

  public RequestResourcesDescription(boolean[] array) {
    this(array.length);
  }

  public RequestResourcesDescription(Result result) {
    this(1);
  }

  public RequestResourcesDescription(Result[] array) {
    this(array.length);
  }
}
