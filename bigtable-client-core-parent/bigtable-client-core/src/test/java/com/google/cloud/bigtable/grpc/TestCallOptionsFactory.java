/*
 * Copyright 2018 Google LLC. All Rights Reserved.
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

import com.google.bigtable.v2.MutateRowRequest;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.cloud.bigtable.config.CallOptionsConfig;
import io.grpc.CallOptions;
import io.grpc.Context;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Tests for {@link CallOptionsFactory}. */
@RunWith(JUnit4.class)
public class TestCallOptionsFactory {

    @Mock
    ScheduledExecutorService mockExecutor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDefault() {
        CallOptionsFactory factory = new CallOptionsFactory.Default();
        Assert.assertSame(CallOptions.DEFAULT, factory.create(null, null));
    }

    @Test
    public void testDefaultWithContext() {
        Context.CancellableContext context = Context.current().withDeadlineAfter(1, TimeUnit.SECONDS, mockExecutor);
        context.run(new Runnable() {
            @Override
            public void run() {
                CallOptionsFactory factory = new CallOptionsFactory.Default();
                Assert.assertEquals(
                        Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS),
                        getDeadlineMs(factory, null),
                        100.0
                );
            }
        });
    }

    @Test
    public void testConfiguredDefaultConfig() {
        CallOptionsConfig config = new CallOptionsConfig.Builder().build();
        CallOptionsFactory factory = new CallOptionsFactory.ConfiguredCallOptionsFactory(config);
        Assert.assertSame(CallOptions.DEFAULT, factory.create(null, null));
    }

    @Test
    public void testConfiguredConfigEnabled() {
        CallOptionsConfig config = new CallOptionsConfig.Builder().setUseTimeout(true).build();
        CallOptionsFactory factory = new CallOptionsFactory.ConfiguredCallOptionsFactory(config);
        Assert.assertEquals(
                (double) config.getShortRpcTimeoutMs(),
                getDeadlineMs(factory, MutateRowRequest.getDefaultInstance()),
                100.0
        );
        Assert.assertEquals(
                (double) config.getLongRpcTimeoutMs(),
                (double) getDeadlineMs(factory, MutateRowsRequest.getDefaultInstance()),
                100.0
        );
    }

    @Test
    public void testConfiguredWithContext() {
        Context.CancellableContext context = Context.current().withDeadlineAfter(1, TimeUnit.SECONDS, mockExecutor);
        context.run(new Runnable() {
            @Override
            public void run() {
                CallOptionsConfig config = new CallOptionsConfig.Builder().setUseTimeout(true).build();
                CallOptionsFactory factory = new CallOptionsFactory.ConfiguredCallOptionsFactory(config);
                Assert.assertEquals(
                        Context.current().getDeadline().timeRemaining(TimeUnit.MILLISECONDS),
                        getDeadlineMs(factory, MutateRowRequest.getDefaultInstance()),
                        100.0
                );
            }
        });
    }

    private double getDeadlineMs(CallOptionsFactory factory, Object request) {
        return (double) factory.create(null, request).getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
    }
}
