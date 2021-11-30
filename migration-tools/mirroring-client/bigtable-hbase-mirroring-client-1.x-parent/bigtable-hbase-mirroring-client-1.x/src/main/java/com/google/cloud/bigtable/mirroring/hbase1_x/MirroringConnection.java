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
package com.google.cloud.bigtable.mirroring.hbase1_x;

import com.google.cloud.bigtable.mirroring.core.MirroringConfiguration;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;

public class MirroringConnection
    extends com.google.cloud.bigtable.mirroring.core.MirroringConnection {

  /**
   * The constructor called from {@link ConnectionFactory#createConnection(Configuration)} and in
   * its many forms via reflection with this specific signature.
   *
   * <p>Parameters are passed down to ConnectionFactory#createConnection method, connection errors
   * are passed back to the user.
   */
  public MirroringConnection(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws Throwable {
    super(conf, managed, pool, user);
  }

  public MirroringConnection(MirroringConfiguration mirroringConfiguration, ExecutorService pool)
      throws IOException {
    super(mirroringConfiguration, pool);
  }
}
