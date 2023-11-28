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
package com.google.cloud.bigtable.mirroring.core;

import com.google.common.util.concurrent.FutureCallback;

/**
 * Write operations do not perform verification, only report failed writes. For this reason callback
 * used in write operations do not need to implement non-trivial {@link #onSuccess(Object)} methods.
 * This class makes this intent explicit.
 */
public abstract class WriteOperationFutureCallback<T> implements FutureCallback<T> {

  @Override
  public final void onSuccess(T t) {}

  public abstract void onFailure(Throwable throwable);
}
