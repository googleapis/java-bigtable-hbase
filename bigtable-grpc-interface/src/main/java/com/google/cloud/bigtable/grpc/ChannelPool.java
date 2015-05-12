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

import io.grpc.Call;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Manages a set of ClosableChannels and uses them in a round robin.
 */
public class ChannelPool extends CloseableChannel {

  protected static final Logger log = Logger.getLogger(ChannelPool.class.getName());

  private final CloseableChannel[] channels;
  private final AtomicInteger requestCount = new AtomicInteger();

  public ChannelPool(CloseableChannel[] channels) {
    this.channels = channels;
  }

  @Override
  public <RequestT, ResponseT> Call<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor) {
    int currentRequestNum = requestCount.getAndIncrement();
    int index = Math.abs(currentRequestNum % channels.length);
    return channels[index].newCall(methodDescriptor);
  }

  @Override
  public void close() throws IOException {
    for (CloseableChannel closeableChannel : channels) {
      closeableChannel.close();
    }
  }
}
