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

import com.google.api.client.util.SecurityUtils;
import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.auth.RequestMetadataCallback;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.bigtable.config.CredentialOptions.JsonCredentialsOptions;
import com.google.cloud.bigtable.config.CredentialOptions.P12CredentialOptions;
import com.google.cloud.bigtable.config.CredentialOptions.UserSuppliedCredentialOptions;
import com.google.cloud.http.HttpTransportOptions.DefaultHttpTransportFactory;
import com.google.common.collect.ImmutableList;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Simple factory for creating OAuth Credential objects for use with Bigtable.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class CredentialFactory {

  /** The OAuth scope required to perform administrator actions such as creating tables. */
  public static final String CLOUD_BIGTABLE_ADMIN_SCOPE =
      "https://www.googleapis.com/auth/cloud-bigtable.admin";
  /** The OAuth scope required to read data from tables. */
  public static final String CLOUD_BIGTABLE_READER_SCOPE =
      "https://www.googleapis.com/auth/cloud-bigtable.data.readonly";
  /** The OAuth scope required to write data to tables. */
  public static final String CLOUD_BIGTABLE_WRITER_SCOPE =
      "https://www.googleapis.com/auth/cloud-bigtable.data";

  /** Scopes required to read and write data from tables. */
  public static final List<String> CLOUD_BIGTABLE_READ_WRITE_SCOPES =
      ImmutableList.of(CLOUD_BIGTABLE_READER_SCOPE, CLOUD_BIGTABLE_WRITER_SCOPE);

  /** Scopes required for full access to cloud bigtable. */
  public static final List<String> CLOUD_BIGTABLE_ALL_SCOPES =
      ImmutableList.of(
          CLOUD_BIGTABLE_READER_SCOPE, CLOUD_BIGTABLE_WRITER_SCOPE, CLOUD_BIGTABLE_ADMIN_SCOPE);

  // HTTP transport used for created credentials to perform token-refresh handshakes with remote
  // credential servers. Initialized lazily to move the possibility of throwing
  // GeneralSecurityException to the time a caller actually tries to get a credential.
  private static HttpTransportFactory httpTransportFactory;

  /** Constant <code>LOG</code> */
  private static final Logger LOG = new Logger(CredentialFactory.class);

  /**
   * Allow for an override of the credentials HttpTransportFactory.
   *
   * @param httpTransportFactory
   */
  public static void setHttpTransportFactory(HttpTransportFactory httpTransportFactory) {
    CredentialFactory.httpTransportFactory = httpTransportFactory;
  }

  public static HttpTransportFactory getHttpTransportFactory() {
    if (httpTransportFactory == null) {
      httpTransportFactory = new DefaultHttpTransportFactory();
    }
    return httpTransportFactory;
  }

  /**
   * Look up a Credentials object based on a configuration of credentials described in a {@link
   * com.google.cloud.bigtable.config.CredentialOptions}.
   *
   * @param options a {@link com.google.cloud.bigtable.config.CredentialOptions} object.
   * @return a {@link com.google.auth.Credentials} object.
   * @throws java.io.IOException if any.
   * @throws java.security.GeneralSecurityException if any.
   */
  public static Credentials getCredentials(CredentialOptions options)
      throws IOException, GeneralSecurityException {

    return patchCredentials(getCredentialsInner(options));
  }

  private static Credentials getCredentialsInner(CredentialOptions options)
      throws IOException, GeneralSecurityException {
    switch (options.getCredentialType()) {
      case DefaultCredentials:
        return getApplicationDefaultCredential();
      case P12:
        P12CredentialOptions p12Options = (P12CredentialOptions) options;
        return getCredentialFromPrivateKeyServiceAccount(
            p12Options.getServiceAccount(), p12Options.getKeyFile());
      case SuppliedCredentials:
        return ((UserSuppliedCredentialOptions) options).getCredential();
      case SuppliedJson:
        JsonCredentialsOptions jsonCredentialsOptions = (JsonCredentialsOptions) options;
        synchronized (jsonCredentialsOptions) {
          if (jsonCredentialsOptions.getCachedCredentials() == null) {
            jsonCredentialsOptions.setCachedCredentails(
                getInputStreamCredential(jsonCredentialsOptions.getInputStream()));
          }
          return jsonCredentialsOptions.getCachedCredentials();
        }
      case None:
        return null;
      default:
        throw new IllegalStateException(
            "Cannot process Credential type: " + options.getCredentialType());
    }
  }

  /**
   * Initializes OAuth2 credential from a private keyfile, as described in <a
   * href="https://developers.google.com/api-client-library/java/google-api-java-client/oauth2#service_accounts"
   * >Service accounts</a>.
   *
   * @param serviceAccountEmail Email address of the service account associated with the keyfile.
   * @param privateKeyFile Full local path to private keyfile.
   * @return a {@link com.google.auth.Credentials} object.
   * @throws java.io.IOException if any.
   * @throws java.security.GeneralSecurityException if any.
   */
  public static Credentials getCredentialFromPrivateKeyServiceAccount(
      String serviceAccountEmail, String privateKeyFile)
      throws IOException, GeneralSecurityException {

    PrivateKey privateKey =
        SecurityUtils.loadPrivateKeyFromKeyStore(
            SecurityUtils.getPkcs12KeyStore(),
            new FileInputStream(privateKeyFile),
            "notasecret",
            "privatekey",
            "notasecret");

    return patchCredentials(
        ServiceAccountJwtAccessCredentials.newBuilder()
            .setClientEmail(serviceAccountEmail)
            .setPrivateKey(privateKey)
            .build());
  }

  /**
   * Initializes OAuth2 credential from a private keyfile, as described in <a
   * href="https://developers.google.com/api-client-library/java/google-api-java-client/oauth2#service_accounts"
   * >Service accounts</a>.
   *
   * @param serviceAccountEmail Email address of the service account associated with the keyfile.
   * @param privateKeyFile Full local path to private keyfile.
   * @param scopes List of well-formed desired scopes to use with the credential.
   * @return a {@link com.google.auth.Credentials} object.
   * @throws java.io.IOException if any.
   * @throws java.security.GeneralSecurityException if any.
   */
  public static Credentials getCredentialFromPrivateKeyServiceAccount(
      String serviceAccountEmail, String privateKeyFile, List<String> scopes)
      throws IOException, GeneralSecurityException {

    PrivateKey privateKey =
        SecurityUtils.loadPrivateKeyFromKeyStore(
            SecurityUtils.getPkcs12KeyStore(),
            new FileInputStream(privateKeyFile),
            "notasecret",
            "privatekey",
            "notasecret");

    // Since the user specified scopes, we can't use JWT tokens
    return patchCredentials(
        ServiceAccountCredentials.newBuilder()
            .setClientEmail(serviceAccountEmail)
            .setPrivateKey(privateKey)
            .setScopes(scopes)
            .setHttpTransportFactory(getHttpTransportFactory())
            .build());
  }

  /**
   * Initializes OAuth2 application default credentials based on the environment the code is running
   * in. If a service account is to be used with JSON file, set the environment variable with name
   * "GOOGLE_APPLICATION_CREDENTIALS" to the JSON file path. For more details on application default
   * credentials: <a
   * href="https://developers.google.com/identity/protocols/application-default-credentials" >
   * Application Default Credentials</a>.
   *
   * @return a {@link com.google.auth.Credentials} object.
   * @throws java.io.IOException if any.
   */
  public static Credentials getApplicationDefaultCredential() throws IOException {
    GoogleCredentials credentials =
        GoogleCredentials.getApplicationDefault(getHttpTransportFactory());

    if (credentials instanceof ServiceAccountCredentials) {
      return getJwtToken((ServiceAccountCredentials) credentials);
    }

    return credentials.createScoped(CLOUD_BIGTABLE_ALL_SCOPES);
  }

  /**
   * Initializes OAuth2 application default credentials based on an inputStream.
   *
   * @param inputStream a {@link java.io.InputStream} object.
   * @return a {@link com.google.auth.Credentials} object.
   * @throws java.io.IOException if any.
   */
  public static Credentials getInputStreamCredential(InputStream inputStream) throws IOException {
    GoogleCredentials credentials =
        GoogleCredentials.fromStream(inputStream, getHttpTransportFactory());

    if (credentials instanceof ServiceAccountCredentials) {
      return getJwtToken((ServiceAccountCredentials) credentials);
    }
    return credentials.createScoped(CLOUD_BIGTABLE_ALL_SCOPES);
  }

  private static Credentials getJwtToken(ServiceAccountCredentials serviceAccount) {
    return patchCredentials(
        ServiceAccountJwtAccessCredentials.newBuilder()
            .setClientEmail(serviceAccount.getClientEmail())
            .setClientId(serviceAccount.getClientId())
            .setPrivateKey(serviceAccount.getPrivateKey())
            .setPrivateKeyId(serviceAccount.getPrivateKeyId())
            .build());
  }

  // TODO(igorbernstein): Remove this ugly hack, once the serverside is fixed.
  /**
   * Workaround for broken JWT tokens in batch-bigtable.googleapis.com.
   *
   * <p>Currently we only accept JWT tokens destined for the audience bigtable.googleapis.com and
   * bigtableadmin.googleapis.com. However, gRPC will set the audience based on the endpoint. This
   * workaround will rewrite the audience to be compliant.
   */
  private static Credentials patchCredentials(Credentials credentials) {
    // Wrap JWT credentials to rewrite the audience.
    if (credentials instanceof ServiceAccountJwtAccessCredentials) {
      credentials =
          new JwtAudienceWorkaroundCredentials((ServiceAccountJwtAccessCredentials) credentials);
    }

    // When dealing with ServiceAccountCredentials, make sure to set the scopes. Otherwise
    // MoreCallCredentials will try to replace it with ServiceAccountJwtAccessCredentials.
    if (credentials instanceof ServiceAccountCredentials) {
      ServiceAccountCredentials svcCreds = (ServiceAccountCredentials) credentials;
      if (svcCreds.getScopes().isEmpty()) {
        credentials = svcCreds.createScoped(CLOUD_BIGTABLE_ALL_SCOPES);
      }
    }

    return credentials;
  }

  private static class JwtAudienceWorkaroundCredentials extends Credentials {
    private final ServiceAccountJwtAccessCredentials inner;

    public JwtAudienceWorkaroundCredentials(ServiceAccountJwtAccessCredentials inner) {
      this.inner = inner;
    }

    @Override
    public String getAuthenticationType() {
      return inner.getAuthenticationType();
    }

    // Extracted from ServiceAccountJwtAccessCredentials to maintain the performance tweak.
    @Override
    public void getRequestMetadata(
        final URI uri, Executor executor, final RequestMetadataCallback callback) {
      // It doesn't use network. Only some CPU work on par with TLS handshake. So it's preferrable
      // to do it in the current thread, which is likely to be the network thread.
      blockingGetToCallback(uri, callback);
    }

    @Override
    public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
      if (BigtableOptions.BIGTABLE_BATCH_DATA_HOST_DEFAULT.equals(uri.getHost())) {
        try {
          uri =
              new URI(
                  uri.getScheme(),
                  BigtableOptions.BIGTABLE_DATA_HOST_DEFAULT,
                  uri.getPath(),
                  uri.getFragment());
        } catch (URISyntaxException e) {
          // Should never happen
          throw new IllegalStateException("Failed to adapt batch endpoint creds uri");
        }
      }

      return inner.getRequestMetadata(uri);
    }

    @Override
    public boolean hasRequestMetadata() {
      return inner.hasRequestMetadata();
    }

    @Override
    public boolean hasRequestMetadataOnly() {
      return inner.hasRequestMetadataOnly();
    }

    @Override
    public void refresh() {
      inner.refresh();
    }
  }
}
