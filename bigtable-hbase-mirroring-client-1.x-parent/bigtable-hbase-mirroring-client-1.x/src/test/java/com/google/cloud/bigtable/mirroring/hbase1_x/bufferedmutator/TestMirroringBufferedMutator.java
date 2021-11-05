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
package com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.mirroring.hbase1_x.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
import com.google.cloud.bigtable.mirroring.hbase1_x.TestConnection;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class TestMirroringBufferedMutator {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExecutorServiceRule executorServiceRule = ExecutorServiceRule.cachedPoolExecutor();

  public final MirroringBufferedMutatorCommon mutatorRule = new MirroringBufferedMutatorCommon();

  @Test
  public void testMirroringBufferedMutatorFactory() throws IOException {
    Configuration testConfiguration = new Configuration(false);
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY,
        TestConnection.class.getCanonicalName());
    testConfiguration.set(
        MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, "default");
    MirroringConfiguration configuration = new MirroringConfiguration(testConfiguration);

    assertThat(
            MirroringBufferedMutator.create(
                false,
                mutatorRule.primaryConnection,
                mutatorRule.secondaryConnection,
                mutatorRule.bufferedMutatorParams,
                configuration,
                mutatorRule.flowController,
                executorServiceRule.executorService,
                mutatorRule.secondaryWriteErrorConsumerWithMetrics,
                new MirroringTracer()))
        .isInstanceOf(SequentialMirroringBufferedMutator.class);

    assertThat(
            MirroringBufferedMutator.create(
                true,
                mutatorRule.primaryConnection,
                mutatorRule.secondaryConnection,
                mutatorRule.bufferedMutatorParams,
                configuration,
                mutatorRule.flowController,
                executorServiceRule.executorService,
                mutatorRule.secondaryWriteErrorConsumerWithMetrics,
                new MirroringTracer()))
        .isInstanceOf(ConcurrentMirroringBufferedMutator.class);
  }
}
