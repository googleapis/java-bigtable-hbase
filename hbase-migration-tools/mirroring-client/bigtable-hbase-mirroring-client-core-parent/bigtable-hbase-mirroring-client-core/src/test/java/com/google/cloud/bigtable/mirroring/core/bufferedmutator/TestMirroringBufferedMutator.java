/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.mirroring.core.bufferedmutator;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import com.google.cloud.bigtable.mirroring.core.ExecutorServiceRule;
import com.google.cloud.bigtable.mirroring.core.MirroringConfiguration;
import com.google.cloud.bigtable.mirroring.core.TestConnection;
import com.google.cloud.bigtable.mirroring.core.utils.MirroringConfigurationHelper;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.NoopTimestamper;
import com.google.cloud.bigtable.mirroring.core.utils.timestamper.Timestamper;
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
  Timestamper timestamper = new NoopTimestamper();

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
                mock(ReferenceCounter.class),
                timestamper,
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
                mock(ReferenceCounter.class),
                timestamper,
                new MirroringTracer()))
        .isInstanceOf(ConcurrentMirroringBufferedMutator.class);
  }
}
