/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam.coders;

import java.util.List;

import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviderRegistrar;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import com.google.auto.service.AutoService;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.common.collect.ImmutableList;

/**
 * A {@link CoderProviderRegistrar} for standard types used with {@link CloudBigtableIO}.
 */
@AutoService(CoderProviderRegistrar.class)
public class CBTIOCoderRegistrar implements CoderProviderRegistrar  {
  @Override
  public List<CoderProvider> getCoderProviders() {
    return ImmutableList.of(
      CoderProviders.forCoder(TypeDescriptor.of(Delete.class), HBaseMutationCoder.of()),
      CoderProviders.forCoder(TypeDescriptor.of(Mutation.class), HBaseMutationCoder.of()),
      CoderProviders.forCoder(TypeDescriptor.of(Put.class), HBaseMutationCoder.of()),
      CoderProviders.forCoder(TypeDescriptor.of(Result.class), HBaseResultCoder.of()));
  }
}
