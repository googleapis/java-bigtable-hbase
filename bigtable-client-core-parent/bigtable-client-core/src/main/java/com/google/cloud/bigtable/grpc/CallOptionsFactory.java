/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.grpc;

import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import io.grpc.CallOptions;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.MethodDescriptor;
import java.util.concurrent.TimeUnit;

/**
 * A factory that creates {@link io.grpc.CallOptions} for use in {@link
 * com.google.cloud.bigtable.grpc.BigtableDataClient} RPCs.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public interface CallOptionsFactory {

  /**
   * Provide a {@link io.grpc.CallOptions} object to be used in a single RPC. {@link
   * io.grpc.CallOptions} can contain state, specifically start time with an expiration is set; in
   * cases when timeouts are used, implementations should create a new CallOptions each time this
   * method is called.
   *
   * @param descriptor The RPC that's being called. Different methods have different performance
   *     characteristics, so this parameter can be useful to craft the right timeout for the right
   *     method.
   * @param request Some methods, specifically ReadRows, can have variability depending on the
   *     request. The request can be for either a single row, or a range. This parameter can be used
   *     to tune timeouts
   * @param <RequestT> a RequestT object.
   * @return a {@link io.grpc.CallOptions} object.
   */
  <RequestT> CallOptions create(MethodDescriptor<RequestT, ?> descriptor, RequestT request);

  /**
   * Returns {@link CallOptions#DEFAULT} with any {@link Context#current()}'s {@link
   * Context#getDeadline()} applied to it.
   */
  public static class Default implements CallOptionsFactory {
    @Override
    public <RequestT> CallOptions create(
        MethodDescriptor<RequestT, ?> descriptor, RequestT request) {
      Deadline contextDeadline = Context.current().getDeadline();
      if (contextDeadline != null) {
        return CallOptions.DEFAULT.withDeadline(contextDeadline);
      } else {
        return CallOptions.DEFAULT;
      }
    }
  }

  /** Creates a new {@link CallOptions} based on a {@link CallOptionsConfig}. */
  public static class ConfiguredCallOptionsFactory implements CallOptionsFactory {
    private final CallOptionsConfig config;

    public ConfiguredCallOptionsFactory(CallOptionsConfig config) {
      this.config = config;
    }

    @Override
    /**
     * Creates a {@link CallOptions} with a focus on {@link Deadlines}. Deadlines are decided in the
     * following order:
     *
     * <ol>
     *   <li>If a user set a {@link Context} deadline (see {@link Context#getDeadline()}), use that
     *   <li>If a user configured deadlines via {@link CallOptionsConfig}, use it.
     *   <li>Otherwise, use {@link CallOptions#DEFAULT}.
     * </ol>
     */
    public <RequestT> CallOptions create(
        MethodDescriptor<RequestT, ?> descriptor, RequestT request) {
      Deadline contextDeadline = Context.current().getDeadline();
      if (contextDeadline != null) {
        return CallOptions.DEFAULT.withDeadline(contextDeadline);
      } else if (config.isUseTimeout() && request != null) {
        int timeout = getRequestTimeout(request);
        return CallOptions.DEFAULT.withDeadline(Deadline.after(timeout, TimeUnit.MILLISECONDS));
      } else {
        return CallOptions.DEFAULT;
      }
    }

    /**
     * @param request an RPC request.
     * @return timeout in milliseconds as per the Request type.
     */
    private int getRequestTimeout(Object request) {
      if (request instanceof ReadRowsRequest && !isGet((ReadRowsRequest) request)) {
        return config.getReadStreamRpcTimeoutMs();
      } else if (request instanceof MutateRowsRequest) {
        return config.getMutateRpcTimeoutMs();
      } else {
        return config.getShortRpcTimeoutMs();
      }
    }

    public static boolean isGet(ReadRowsRequest request) {
      RowSet rowSet = request.getRows();
      return rowSet != null && rowSet.getRowRangesCount() == 0 && rowSet.getRowKeysCount() == 1;
    }
  }
}
