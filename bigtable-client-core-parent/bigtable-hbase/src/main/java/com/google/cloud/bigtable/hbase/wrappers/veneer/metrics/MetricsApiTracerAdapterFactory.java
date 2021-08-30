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
package com.google.cloud.bigtable.hbase.wrappers.veneer.metrics;

import com.google.api.core.InternalApi;
import com.google.api.gax.tracing.ApiTracer;
import com.google.api.gax.tracing.ApiTracerFactory;
import com.google.api.gax.tracing.SpanName;
import com.google.cloud.bigtable.metrics.RpcMetrics;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;

/*
 * Implementation of ApiTracerFactory to bridge veneer metrics
 */
@InternalApi
public class MetricsApiTracerAdapterFactory implements ApiTracerFactory {

  private final Map<String, RpcMetrics> methodMetrics = new HashMap<>();

  @Override
  public ApiTracer newTracer(ApiTracer parent, SpanName spanName, OperationType operationType) {
    RpcMetrics rpcMetrics = getRpcMetrics(spanName);
    return new MetricsApiTracerAdapter(rpcMetrics, spanName.getMethodName(), operationType);
  }

  @VisibleForTesting
  public Map<String, RpcMetrics> getMethodMetrics() {
    return methodMetrics;
  }

  private RpcMetrics getRpcMetrics(SpanName spanName) {
    String key = spanName.getMethodName();

    RpcMetrics rpcMetrics = this.methodMetrics.get(key);
    if (rpcMetrics != null) {
      return rpcMetrics;
    }

    synchronized (this) {
      rpcMetrics = this.methodMetrics.get(key);
      if (rpcMetrics != null) {
        return rpcMetrics;
      }
      rpcMetrics = RpcMetrics.createRpcMetrics(key);
      this.methodMetrics.put(key, rpcMetrics);
      return rpcMetrics;
    }
  }
}
