/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.api.client.util.ExponentialBackOff;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import io.grpc.Call;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.MethodType;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@link Channel} that retries unary calls when an internal error occurs.
 */
public class UnaryCallRetryInterceptor extends Channel {

  private final Channel delegate;
  private final ScheduledExecutorService executorService;
  private final Map<MethodDescriptor<?, ?>, Predicate<? extends Object>> retriableMethods;
  private final int initialBackoffMillis;
  private final double backoffMultiplier;
  private final int maxElapsedBackoffMillis;

  public UnaryCallRetryInterceptor(
      Channel delegate,
      ScheduledExecutorService executorService,
      Set<MethodDescriptor<?, ?>> retriableMethods,
      int initialBackoffMillis,
      double backoffMultiplier,
      int maxElapsedBackoffMillis) {
    this(
        delegate,
        executorService,
        Maps.asMap(retriableMethods, new Function<MethodDescriptor<?, ?>, Predicate<?>>() {
          @Override
          public Predicate<Object> apply(MethodDescriptor<?, ?> methodDescriptor) {
            return Predicates.alwaysTrue();
          }
        }),
        initialBackoffMillis,
        backoffMultiplier,
        maxElapsedBackoffMillis);
  }

  public UnaryCallRetryInterceptor(
      Channel delegate,
      ScheduledExecutorService executorService,
      Map<MethodDescriptor<?, ?>, Predicate<?>> retriableMethods,
      int initialBackoffMillis,
      double backoffMultiplier,
      int maxElapsedBackoffMillis) {
    this.delegate = delegate;
    this.executorService = executorService;
    this.retriableMethods = retriableMethods;
    this.initialBackoffMillis = initialBackoffMillis;
    this.backoffMultiplier = backoffMultiplier;
    this.maxElapsedBackoffMillis = maxElapsedBackoffMillis;
  }

  @Override
  public <ReqT, RespT> Call<ReqT, RespT> newCall(MethodDescriptor<ReqT, RespT> methodDescriptor) {
    if (methodCanBeRetried(methodDescriptor)) {
      ExponentialBackOff.Builder backOffBuilder = new ExponentialBackOff.Builder();
      backOffBuilder.setInitialIntervalMillis(initialBackoffMillis);
      backOffBuilder.setMultiplier(backoffMultiplier);
      backOffBuilder.setMaxElapsedTimeMillis(maxElapsedBackoffMillis);
      Predicate<ReqT> isPayloadRetriablePredicate = getUncheckedPredicate(methodDescriptor);
      return new RetryingCall<>(
          delegate,
          methodDescriptor,
          isPayloadRetriablePredicate,
          executorService,
          backOffBuilder.build());
    }
    return delegate.newCall(methodDescriptor);
  }

  @SuppressWarnings("unchecked")
  private <RequestT> Predicate<RequestT> getUncheckedPredicate(
      MethodDescriptor<RequestT, ?> method) {
    return (Predicate<RequestT>) retriableMethods.get(method);
  }

  private boolean methodCanBeRetried(MethodDescriptor<?, ?> methodDescriptor) {
    return methodDescriptor.getType() == MethodType.UNARY
        && retriableMethods.containsKey(methodDescriptor);
  }
}
