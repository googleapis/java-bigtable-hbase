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
package com.google.cloud.bigtable.util;

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialFactory;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.CredentialOptions.CredentialType;
import com.google.cloud.bigtable.config.Logger;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;

@InternalApi("For internal use only")
public class DirectPathUtil {
  private static final Logger LOG = new Logger(DirectPathUtil.class);

  private static final String GCE_PRODUCTION_NAME_PRIOR_2016 = "Google";
  private static final String GCE_PRODUCTION_NAME_AFTER_2016 = "Google Compute Engine";

  private static final Supplier<Boolean> isOnComputeEngine =
      Suppliers.memoize(
          new Supplier<Boolean>() {
            @Override
            public Boolean get() {
              String osName = System.getProperty("os.name");
              if ("Linux".equals(osName)) {
                try {
                  String result =
                      Files.asCharSource(
                              new File("/sys/class/dmi/id/product_name"), StandardCharsets.UTF_8)
                          .readFirstLine();

                  return result != null
                      && (result.contains(GCE_PRODUCTION_NAME_PRIOR_2016)
                          || result.contains(GCE_PRODUCTION_NAME_AFTER_2016));
                } catch (IOException e) {
                  return false;
                }
              }
              return false;
            }
          });

  private DirectPathUtil() {}

  public static boolean shouldAttemptDirectPath(
      String endpoint, int port, CredentialOptions creds) {

    return BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT.equals(endpoint)
        && BigtableOptions.BIGTABLE_PORT_DEFAULT == port
        && isOnComputeEngine.get()
        && areCredsDirectPathCompatible(creds);
  }

  private static boolean areCredsDirectPathCompatible(CredentialOptions credentialOptions) {
    if (credentialOptions.getCredentialType() != CredentialType.DefaultCredentials) {
      return false;
    }
    Credentials credentials = null;
    try {
      credentials = CredentialFactory.getCredentials(credentialOptions);
    } catch (IOException | GeneralSecurityException e) {
      LOG.warn("Failed to probe credentials, assuming they are not DirectPath compatible", e);
    }
    return credentials instanceof ComputeEngineCredentials;
  }
}
