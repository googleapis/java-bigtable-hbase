/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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

import java.io.IOException;

import com.google.cloud.bigtable.config.CredentialOptions;
import io.grpc.netty.shaded.io.netty.handler.ssl.OpenSsl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.cloud.bigtable.config.BigtableOptions;

@SuppressWarnings({"resource","unused"})
public class TestBigtableSession {

  private static final String PROJECT_ID = "project_id";
  private static final String INSTANCE_ID = "instance_id";
  private static final String USER_AGENT = "user_agent";

  private static void createSession(String projectId, String instanceId, String userAgent)
      throws IOException {
    BigtableSession ignored =
        new BigtableSession(BigtableOptions.builder()
          .setProjectId(projectId)
          .setInstanceId(instanceId)
          .setUserAgent(userAgent)
          .build());
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testNoProjectIdBigtableOptions() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(BigtableSession.PROJECT_ID_EMPTY_OR_NULL);
    createSession(null, INSTANCE_ID, USER_AGENT);
  }

  @Test
  public void testNoInstanceIdBigtableOptions() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(BigtableSession.INSTANCE_ID_EMPTY_OR_NULL);
    createSession(PROJECT_ID, null, USER_AGENT);
  }

  @Test
  public void testNoUserAgentBigtableOptions() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(BigtableSession.USER_AGENT_EMPTY_OR_NULL);
    createSession(PROJECT_ID, INSTANCE_ID, null);
  }

  @Test
  public void testOpenSSL() throws Throwable {
    if (!OpenSsl.isAvailable()) {
      throw OpenSsl.unavailabilityCause();
    }
  }

  /**
   * Test to make sure that {@link BigtableSession#createInstanceClient()} can be created.
   * @throws Throwable
   */
  @Test
  public void testCreateInstanceClient() throws Throwable {
    try {
      BigtableSession.createInstanceClient(BigtableOptions.builder().setCredentialOptions(
          CredentialOptions.nullCredential()).build());
    } catch (IOException e) {
      if (e.getMessage().toLowerCase().contains("credentials")) {
        // ignore;  This is running on a system that doesn't have default credentails,
        // and this test is to ensure that openssl works well.  Travis isn't sent up with
        // credentials, so this test should pass in those conditions.
      } else {
        throw e;
      }
    }
  }

}
