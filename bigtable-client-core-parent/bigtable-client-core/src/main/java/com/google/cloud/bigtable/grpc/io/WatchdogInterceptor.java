package com.google.cloud.bigtable.grpc.io;

import com.google.api.core.InternalApi;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import java.util.Set;

/**
 * Internal implementation detail to prevent RPC from hanging
 */
@InternalApi
public class WatchdogInterceptor implements ClientInterceptor {
  private final Set<MethodDescriptor<?,?>> watchedMethods;
  private final Watchdog watchdog;


  public WatchdogInterceptor(Set<MethodDescriptor<?, ?>> watchedMethods, Watchdog watchdog) {
    this.watchedMethods = watchedMethods;
    this.watchdog = watchdog;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

    ClientCall<ReqT, RespT> call = channel.newCall(methodDescriptor, callOptions);
    if (watchedMethods.contains(methodDescriptor)) {
      call = watchdog.watch(call);
    }

    return call;
  }
}
