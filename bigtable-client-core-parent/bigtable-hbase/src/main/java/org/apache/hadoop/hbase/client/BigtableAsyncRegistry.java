/*
 * Copyright 2017 Google LLC
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
package org.apache.hadoop.hbase.client;

import com.google.api.core.InternalApi;
import java.util.concurrent.CompletableFuture;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.loading.InjectionClassLoader;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.hadoop.conf.Configuration;

/**
 * Bigtable implementation of org.apache.hadoop.hbase.client.ConnectionRegistry or
 * org.apache.hadoop.hbase.client.AsyncRegistry depending on the HBase 2 version. The default HBase
 * 2 implementation provided by {@link ZKConnectionRegistry} assumes a ZooKeeper environment, which
 * is not the case for Bigtable.
 *
 * <p>This class is injected via the system property: "hbase.client.registry.impl" For further
 * details See {@link HConstants#CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY}, and {@link
 * ConnectionFactory#createAsyncConnection()}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableAsyncRegistry {

  private static BigtableAsyncRegistry subClass = null;

  public BigtableAsyncRegistry(Configuration conf) {}

  public static BigtableAsyncRegistry getSubClass(Configuration conf) {
    if (subClass == null) {
      try {
        subClass =
            new ByteBuddy()
                .subclass(BigtableAsyncRegistry.class)
                .implement(Class.forName("org.apache.hadoop.hbase.client.ConnectionRegistry"))
                .method(ElementMatchers.isAbstract())
                .intercept(
                    InvocationHandlerAdapter.of(
                        new AbstractBigtableAdmin.UnsupportedOperationsHandler()))
                .method(ElementMatchers.named("close"))
                .intercept(
                    MethodCall.invoke(BigtableAsyncRegistry.class.getDeclaredMethod("closeNoop")))
                .method(ElementMatchers.named("getClusterId"))
                .intercept(FixedValue.value((CompletableFuture.completedFuture("NoopClusterId"))))
                .make()
                .load(
                    InjectionClassLoader.getSystemClassLoader(),
                    ClassLoadingStrategy.Default.INJECTION)
                .getLoaded()
                .getDeclaredConstructor(Configuration.class)
                .newInstance(conf);
      } catch (Exception e) {
      }
      try {
        subClass =
            new ByteBuddy()
                .subclass(BigtableAsyncRegistry.class)
                .implement(Class.forName("org.apache.hadoop.hbase.client.AsyncRegistry"))
                .method(ElementMatchers.isAbstract())
                .intercept(
                    InvocationHandlerAdapter.of(
                        new AbstractBigtableAdmin.UnsupportedOperationsHandler()))
                .method(ElementMatchers.named("close"))
                .intercept(
                    MethodCall.invoke(BigtableAsyncRegistry.class.getDeclaredMethod("closeNoop")))
                .method(ElementMatchers.named("getClusterId"))
                .intercept(FixedValue.value((CompletableFuture.completedFuture("NoopClusterId"))))
                .make()
                .load(
                    InjectionClassLoader.getSystemClassLoader(),
                    ClassLoadingStrategy.Default.INJECTION)
                .getLoaded()
                .getDeclaredConstructor(Configuration.class)
                .newInstance(conf);
      } catch (Exception e) {
      }
    }
    return subClass;
  }

  public void closeNoop() {}
}
