/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.config;

import java.io.InputStream;

import com.google.auth.Credentials;

/**
 * <p>
 * This class encapsulates the method the Cloud Bigtable client should use to look up
 * {@link Credentials}. Here are the credential types supported:
 * </p>
 * <ol>
 * <li>DefaultCredentials - Initializes OAuth2 credential using preconfigured ServiceAccount
 * settings on the local GCE VM or GOOGLE_APPLICATION_CREDENTIALS environment variable or gcloud configured security. See: <a
 * href="https://developers.google.com/compute/docs/authentication">Authenticating from Google
 * Compute Engine</a> and <a
 * href="https://developers.google.com/accounts/docs/application-default-credentials" > Application
 * Default Credentials</a>.</li>
 * <li>P12 - Initializes OAuth2 credential from a private keyfile, as described in <a
 * href="https://code.google.com/p/google-api-java-client/wiki/OAuth2#Service_Accounts" > OAuth2
 * Service Accounts</a>.</li>
 * <li>UserSupplied - User supplies a fully formed Credentials.
 * <li>None - used for unit testing</li>
 * </ol>
 * @see CredentialFactory#getCredentials(CredentialOptions) CredentialFactory for more details on
 *      how CredentialOptions are used to create a {@link Credentials}.
 */
public class CredentialOptions {
  public static final String SERVICE_ACCOUNT_JSON_ENV_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS";
  protected static final Logger LOG = new Logger(CredentialOptions.class);

  public enum CredentialType {
    DefaultCredentials,
    P12,
    SuppliedCredentials,
    SuppliedJson,
    None,
  }

  public static CredentialOptions jsonCredentials(InputStream jsonInputStream) {
    return new JsonCredentialsOptions(jsonInputStream);
  }

  /**
   * Get a configured json credentials file from the GOOGLE_APPLICATION_CREDENTIALS environment
   * variable.
   */
  public static String getEnvJsonFile() {
    return System.getenv().get(SERVICE_ACCOUNT_JSON_ENV_VARIABLE);
  }

  /**
   * <p>
   * Use the Application Default Credentials which are credentials that identify and authorize the
   * whole application. This is the built-in service account if running on Google Compute Engine.
   * Alternatively, the credentials file from the path in the environment variable
   * GOOGLE_APPLICATION_CREDENTIAL. If GOOGLE_APPLICATION_CREDENTIAL is not set, look at the
   * gcloud/application_default_credentials.json file in the (User)/APPDATA/ directory on Windows or
   * ~/.config/ directory on other OSs .
   * </p>
   * Initializes OAuth2 credential using preconfigured ServiceAccount settings on the local GCE VM.
   * See: <a href="https://developers.google.com/compute/docs/authentication">Authenticating from
   * Google Compute Engine</a>.
   */
  public static CredentialOptions defaultCredentials() {
    return new CredentialOptions(CredentialType.DefaultCredentials);
  }

  /**
   * Initializes OAuth2 credential from a private keyfile, as described in <a
   * href="https://code.google.com/p/google-api-java-client/wiki/OAuth2#Service_Accounts" > OAuth2
   * Service Accounts</a>
   */
  public static CredentialOptions p12Credential(String serviceAccount, String keyFile) {
    return new P12CredentialOptions(serviceAccount, keyFile);
  }

  /**
   * A CredentialOption that wraps an existing {@link Credentials} object.
   */
  public static CredentialOptions credential(Credentials credentials) {
    return new UserSuppliedCredentialOptions(credentials);
  }

  /**
   * No credentials - used for unit testing.
   */
  public static CredentialOptions nullCredential() {
    LOG.info("Enabling the use of null credentials. This should not be used in production.");
    return new CredentialOptions(CredentialType.None);
  }

  /**
   * A CredentialOptions defined by a serviceAccount and a p12 key file.
   */
  public static class P12CredentialOptions extends CredentialOptions {
    private final String serviceAccount;
    private final String keyFile;

    private P12CredentialOptions(String serviceAccount, String keyFile) {
      super(CredentialType.P12);
      this.serviceAccount = serviceAccount;
      this.keyFile = keyFile;
    }

    /**
     * ServiceAccount email address used for the P12 file -
     * https://developers.google.com/identity/protocols/OAuth2ServiceAccount
     */
    public String getServiceAccount() {
      return serviceAccount;
    }

    /**
     * P12 file -
     * https://developers.google.com/identity/protocols/OAuth2ServiceAccount
     */
    public String getKeyFile() {
      return keyFile;
    }
  }

  /**
   * A CredentialOption that supplies the Credentials directly.
   */
  public static class UserSuppliedCredentialOptions extends CredentialOptions {
    private final Credentials credential;

    public UserSuppliedCredentialOptions(Credentials credential) {
      super(CredentialType.SuppliedCredentials);
      this.credential = credential;
    }

    public Credentials getCredential() {
      return credential;
    }
  }

  /**
   * A CredentialOption that has a json credentials configured as an InputStream instead of a system
   * environment property.
   */
  public static class JsonCredentialsOptions extends CredentialOptions {
    private final InputStream inputStream;

    public JsonCredentialsOptions(InputStream inputStream) {
      super(CredentialType.SuppliedJson);
      this.inputStream = inputStream;
    }

    public InputStream getInputStream() {
      return inputStream;
    }
  }

  private final CredentialType credentialType;

  private CredentialOptions(CredentialType credentialType) {
    this.credentialType = credentialType;
  }

  public CredentialType getCredentialType() {
    return credentialType;
  }

}
