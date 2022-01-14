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
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.DynamicType;
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
 * 2 implementation provided by ZKAsyncRegistry / ZKConnectionRegistry assumes a ZooKeeper
 * environment, which is not the case for Bigtable.
 *
 * <p>This class is injected via the system property: "hbase.client.registry.impl". For further
 * details See HConstants#CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
 * AsyncRegistry#createAsyncConnection() for hbase < 2.3 and
 * ConnectionFactory#createAsyncConnection() for hbase >= 2.3.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class BigtableAsyncRegistry {

  private static Class<? extends BigtableAsyncRegistry> subClass = null;

  /** Constructor for org.apache.hadoop.hbase.client.ZKConnectionRegistry for hbase >= 2.3. */
  public BigtableAsyncRegistry() {}

  /**
   * Constructor for org.apache.hadoop.hbase.client.ZKAsyncRegistry for hbase < 2.3. Configuration
   * is not used in BigtableAsyncRegistry.
   */
  public BigtableAsyncRegistry(Configuration conf) {}

  /**
   * Implements the async connection registry class depending on which class is present in the class
   * path. #close and #getClusterId are implemented. #getClusterId is used in
   * ConnectionFactory#getAsyncConnection, so a non null return value is required for successful
   * creation of asyncConnection. Throws {@link UnsupportedOperationException} on all other methods.
   */
  private static Class<? extends BigtableAsyncRegistry> createSubClass()
      throws NoSuchMethodException {
    List<String> classNames =
        ImmutableList.of(
            "org.apache.hadoop.hbase.client.ConnectionRegistry",
            "org.apache.hadoop.hbase.client.AsyncRegistry");

    DynamicType.Builder<BigtableAsyncRegistry> subclassBuilder =
        new ByteBuddy().subclass(BigtableAsyncRegistry.class);

    for (String className : classNames) {
      try {
        subclassBuilder = subclassBuilder.implement(Class.forName(className));
      } catch (ClassNotFoundException e) {
        continue;
      }
    }

    return subclassBuilder
        .method(ElementMatchers.isAbstract())
        .intercept(
            InvocationHandlerAdapter.of(new AbstractBigtableAdmin.UnsupportedOperationsHandler()))
        .method(ElementMatchers.named("close"))
        .intercept(MethodCall.invoke(BigtableAsyncRegistry.class.getDeclaredMethod("closeNoop")))
        .method(ElementMatchers.named("getClusterId"))
        .intercept(FixedValue.value((CompletableFuture.completedFuture("NoopClusterId"))))
        .make()
        .load(InjectionClassLoader.getSystemClassLoader(), ClassLoadingStrategy.Default.INJECTION)
        .getLoaded();
  }

  public static synchronized Class<? extends BigtableAsyncRegistry> getSubClass() {
    if (subClass == null) {
      try {
        subClass = createSubClass();
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
      if (subClass == null) {
        throw new RuntimeException(
            "Couldn't find org.apache.hadoop.hbase.client.ConnectionRegistry or org.apache.hadoop.hbase.client.AsyncRegistry in the classpath");
      }
    }
    return subClass;
  }

  public void closeNoop() {}
}
