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
package com.google.cloud.bigtable.config;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.google.auth.Credentials;

/**
 * <p>
 * This class encapsulates the method the Cloud Bigtable client should use to look up
 * {@link com.google.auth.Credentials}. Here are the credential types supported:
 * </p>
 * <ol>
 * <li>DefaultCredentials - Initializes OAuth2 credential using preconfigured ServiceAccount
 * settings on the local Google Compute Engine instance or GOOGLE_APPLICATION_CREDENTIALS
 * environment variable or gcloud configured security. See: <a
 * href="https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances"
 * >Creating and Enabling Service Accounts for Instances</a> and <a
 * href="https://developers.google.com/identity/protocols/application-default-credentials"
 * >Application Default Credentials</a>.</li>
 * <li>P12 - Initializes OAuth2 credential from a private keyfile, as described in <a
 * href="https://developers.google.com/api-client-library/java/google-api-java-client/oauth2#service_accounts"
 * >Service accounts</a>.</li>
 * <li>UserSupplied - User supplies a fully formed Credentials.
 * <li>None - used for unit testing</li>
 * </ol>
 *
 * @see CredentialFactory#getCredentials(CredentialOptions) CredentialFactory for more details on
 *      how CredentialOptions are used to create a {@link com.google.auth.Credentials}.
 * @author sduskis
 * @version $Id: $Id
 */
public class CredentialOptions implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Constant <code>SERVICE_ACCOUNT_JSON_ENV_VARIABLE="GOOGLE_APPLICATION_CREDENTIALS"</code> */
  public static final String SERVICE_ACCOUNT_JSON_ENV_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS";
  /** Constant <code>LOG</code> */
  protected static final Logger LOG = new Logger(CredentialOptions.class);

  public enum CredentialType {
    DefaultCredentials,
    P12,
    SuppliedCredentials,
    SuppliedJson,
    None,
  }

  /**
   * <p>jsonCredentials.</p>
   *
   * @param jsonInputStream a {@link java.io.InputStream} object.
   * @return a {@link com.google.cloud.bigtable.config.CredentialOptions} object.
   */
  public static CredentialOptions jsonCredentials(InputStream jsonInputStream) {
    return new JsonCredentialsOptions(jsonInputStream);
  }

  /**
   * <p>
   * jsonCredentials.
   * </p>
   * @param jsonString a {@link String} object.
   * @return a {@link com.google.cloud.bigtable.config.CredentialOptions} object.
   */
  public static CredentialOptions jsonCredentials(String jsonString) {
    return jsonCredentials(new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * Get a configured json credentials file from the GOOGLE_APPLICATION_CREDENTIALS environment
   * variable.
   *
   * @return a {@link java.lang.String} object.
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
   * Initializes OAuth2 credential using preconfigured ServiceAccount settings on the local Google
   * Compute Engine VM. See:
   * <a href="https://cloud.google.com/compute/docs/access/create-enable-service-accounts-for-instances"
   * >Creating and Enabling Service Accounts for Instances</a>.
   *
   * @return a {@link com.google.cloud.bigtable.config.CredentialOptions} object.
   */
  public static CredentialOptions defaultCredentials() {
    return new CredentialOptions(CredentialType.DefaultCredentials);
  }

  /**
   * Initializes OAuth2 credential from a private keyfile, as described in <a
   * href="https://developers.google.com/api-client-library/java/google-api-java-client/oauth2#service_accounts"
   * >Service accounts</a>.
   *
   * @param serviceAccount a {@link java.lang.String} object.
   * @param keyFile a {@link java.lang.String} object.
   * @return a {@link com.google.cloud.bigtable.config.CredentialOptions} object.
   */
  public static CredentialOptions p12Credential(String serviceAccount, String keyFile) {
    return new P12CredentialOptions(serviceAccount, keyFile);
  }

  /**
   * A CredentialOption that wraps an existing {@link com.google.auth.Credentials} object.
   *
   * @param credentials a {@link com.google.auth.Credentials} object.
   * @return a {@link com.google.cloud.bigtable.config.CredentialOptions} object.
   */
  public static CredentialOptions credential(Credentials credentials) {
    return new UserSuppliedCredentialOptions(credentials);
  }

  /**
   * No credentials - used for unit testing.
   *
   * @return a {@link com.google.cloud.bigtable.config.CredentialOptions} object.
   */
  public static CredentialOptions nullCredential() {
    LOG.info("Enabling the use of null credentials. This should not be used in production.");
    return new CredentialOptions(CredentialType.None);
  }

  /**
   * A CredentialOptions defined by a serviceAccount and a p12 key file.
   */
  public static class P12CredentialOptions extends CredentialOptions {
    private static final long serialVersionUID = 596647835888116163L;
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

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj.getClass() != P12CredentialOptions.class) {
        return false;
      }
      P12CredentialOptions other = (P12CredentialOptions) obj;
      return Objects.equals(keyFile, other.keyFile)
          && Objects.equals(serviceAccount, other.serviceAccount);
    }
  }

  /**
   * A CredentialOption that supplies the Credentials directly.
   */
  public static class UserSuppliedCredentialOptions extends CredentialOptions {
    private static final long serialVersionUID = -7167146778823641468L;
    private final Credentials credential;

    public UserSuppliedCredentialOptions(Credentials credential) {
      super(CredentialType.SuppliedCredentials);
      this.credential = credential;
    }

    public Credentials getCredential() {
      return credential;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj.getClass() != UserSuppliedCredentialOptions.class) {
        return false;
      }
      UserSuppliedCredentialOptions other = (UserSuppliedCredentialOptions) obj;
      return Objects.equals(credential, other.credential);
    }
  }

  /**
   * A CredentialOption that has a json credentials configured as an InputStream instead of a system
   * environment property.
   */
  public static class JsonCredentialsOptions extends CredentialOptions {
    private static final long serialVersionUID = -7868808741264867962L;
    private final InputStream inputStream;
    private Credentials cachedCredentials;

    public JsonCredentialsOptions(InputStream inputStream) {
      super(CredentialType.SuppliedJson);
      this.inputStream = inputStream;
    }

    public InputStream getInputStream() {
      return inputStream;
    }

    public void setCachedCredentails(Credentials credentials) {
      this.cachedCredentials = credentials;
    }

    public Credentials getCachedCredentials() {
      return cachedCredentials;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || obj.getClass() != JsonCredentialsOptions.class) {
        return false;
      }
      JsonCredentialsOptions other = (JsonCredentialsOptions) obj;
      return Objects.equals(cachedCredentials, other.cachedCredentials);
    }
  }

  private CredentialType credentialType;

  private CredentialOptions(CredentialType credentialType) {
    this.credentialType = credentialType;
  }

  private CredentialOptions() {
  }

  /**
   * <p>Getter for the field <code>credentialType</code>.</p>
   *
   * @return a {@link com.google.cloud.bigtable.config.CredentialOptions.CredentialType} object.
   */
  public CredentialType getCredentialType() {
    return credentialType;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != CredentialOptions.class){
      return false;
    }
    CredentialOptions other = (CredentialOptions) obj;
    return credentialType == other.credentialType;
  }
}
