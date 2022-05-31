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
package com.google.cloud.bigtable.mirroring.core.utils.faillog;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class FailedMutationLoggerTest {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();
  @Mock Serializer serializer;
  @Mock Appender appender;

  @Test
  public void mutationsAreSerializedAndAppended() throws Exception {
    try (FailedMutationLogger failedMutationLogger =
        new FailedMutationLogger(appender, serializer)) {
      try {
        throw new RuntimeException("OMG!");
      } catch (RuntimeException e) {
        failedMutationLogger.mutationFailed(new Put(new byte[] {'r'}), e);
      }
    }
    verify(serializer, times(1))
        .serialize(ArgumentMatchers.<Mutation>any(), ArgumentMatchers.<Throwable>any());
    verify(appender, times(1)).append(ArgumentMatchers.<byte[]>any());
    verify(appender, times(1)).close();
  }
}
