/*
 * Copyright 2021 Google Inc.
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
package com.google.cloud.bigtable.beam.sequencefiles;

import static com.google.common.truth.Truth.assertThat;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UtilsTest {
  @Test
  @SuppressWarnings("deprecation")
  public void testRegionIsBackfilledFromZone() {
    DataflowPipelineOptions opts =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);

    opts.setRunner(DataflowRunner.class);
    opts.setZone("us-east1-c");
    opts = Utils.tweakOptions(opts).as(DataflowPipelineOptions.class);
    assertThat(opts.getRegion()).isEqualTo("us-east1");
  }

  @Test
  public void testRegionIsBackfilledFromWorkerZone() {
    DataflowPipelineOptions opts =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);

    opts.setRunner(DataflowRunner.class);
    opts.setWorkerZone("us-east1-c");
    opts = Utils.tweakOptions(opts).as(DataflowPipelineOptions.class);
    assertThat(opts.getRegion()).isEqualTo("us-east1");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testRegionIsNotClobberedByZone() {
    DataflowPipelineOptions opts =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);

    opts.setRunner(DataflowRunner.class);
    opts.setZone("us-east1-c");
    opts.setRegion("us-west1");
    opts = Utils.tweakOptions(opts).as(DataflowPipelineOptions.class);
    assertThat(opts.getRegion()).isEqualTo("us-west1");
  }

  @Test
  public void testRegionIsNotClobberedByWorkerZone() {
    DataflowPipelineOptions opts =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);

    opts.setRunner(DataflowRunner.class);
    opts.setWorkerZone("us-east1-c");
    opts.setRegion("us-west1");
    opts = Utils.tweakOptions(opts).as(DataflowPipelineOptions.class);
    assertThat(opts.getRegion()).isEqualTo("us-west1");
  }
}
