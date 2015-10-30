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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.List;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.SecurityUtils;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigtable.config.CredentialOptions.JsonCredentialsOptions;
import com.google.cloud.bigtable.config.CredentialOptions.P12CredentialOptions;
import com.google.cloud.bigtable.config.CredentialOptions.UserSuppliedCredentialOptions;
import com.google.common.collect.ImmutableList;

/**
 * Simple factory for creating OAuth Credential objects for use with Bigtable.
 */
public class CredentialFactory {

  /**
   * The OAuth scope required to perform administrator actions such as creating tables.
   */
  public static final String CLOUD_BIGTABLE_ADMIN_SCOPE =
      "https://www.googleapis.com/auth/cloud-bigtable.admin";
  /**
   * The OAuth scope required to read data from tables.
   */
  public static final String CLOUD_BIGTABLE_READER_SCOPE =
      "https://www.googleapis.com/auth/cloud-bigtable.data.readonly";
  /**
   * The OAuth scope required to write data to tables.
   */
  public static final String CLOUD_BIGTABLE_WRITER_SCOPE =
      "https://www.googleapis.com/auth/cloud-bigtable.data";

  /**
   * Scopes required to read and write data from tables.
   */
  public static final List<String> CLOUD_BIGTABLE_READ_WRITE_SCOPES =
      ImmutableList.of(
          CLOUD_BIGTABLE_READER_SCOPE,
          CLOUD_BIGTABLE_WRITER_SCOPE);

  /**
   * Scopes required for full access to cloud bigtable.
   */
  public static final List<String> CLOUD_BIGTABLE_ALL_SCOPES =
      ImmutableList.of(
          CLOUD_BIGTABLE_READER_SCOPE,
          CLOUD_BIGTABLE_WRITER_SCOPE,
          CLOUD_BIGTABLE_ADMIN_SCOPE);

  // HTTP transport used for created credentials to perform token-refresh handshakes with remote
  // credential servers. Initialized lazily to move the possibility of throwing
  // GeneralSecurityException to the time a caller actually tries to get a credential.
  private static HttpTransport httpTransport = null;

  /**
   * Returns shared httpTransport instance; initializes httpTransport if it hasn't already been
   * initialized.
   */
  public static synchronized HttpTransport getHttpTransport()
      throws IOException, GeneralSecurityException {
    if (httpTransport == null) {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    }
    return httpTransport;
  }

  /**
   * Look up a Credentials object based on a configuration of credentials described in a
   * {@link CredentialOptions}.
   */
  public static Credentials getCredentials(CredentialOptions options) throws IOException,
      GeneralSecurityException {
    switch (options.getCredentialType()) {
    case DefaultCredentials:
      return getApplicationDefaultCredential();
    case P12:
      P12CredentialOptions p12Options = (P12CredentialOptions) options;
      return getCredentialFromPrivateKeyServiceAccount(p12Options.getServiceAccount(),
        p12Options.getKeyFile());
    case SuppliedCredentials:
      return ((UserSuppliedCredentialOptions) options).getCredential();
    case SuppliedJson:
      JsonCredentialsOptions jsonCredentialsOptions = (JsonCredentialsOptions) options;
      synchronized (jsonCredentialsOptions) {
        if (jsonCredentialsOptions.getCachedCredentials() == null) {
          jsonCredentialsOptions
              .setCachedCredentails(getInputStreamCredential(jsonCredentialsOptions
                  .getInputStream()));
        }
        return jsonCredentialsOptions.getCachedCredentials();
      }
    case None:
      return null;
    default:
      throw new IllegalStateException("Cannot process Credential type: "
          + options.getCredentialType());
    }
  }

  /**
   * Initializes OAuth2 credential using preconfigured ServiceAccount settings on the local
   * GCE VM. See: <a href="https://developers.google.com/compute/docs/authentication"
   * >Authenticating from Google Compute Engine</a>.
   */
  public static Credentials getCredentialFromMetadataServiceAccount()
      throws IOException, GeneralSecurityException {
    return new ComputeEngineCredentials(getHttpTransport());
  }

  /**
   * Initializes OAuth2 credential from a private keyfile, as described in
   * <a href="https://code.google.com/p/google-api-java-client/wiki/OAuth2#Service_Accounts"
   * > OAuth2 Service Accounts</a>.
   *
   * @param serviceAccountEmail Email address of the service account associated with the keyfile.
   * @param privateKeyFile Full local path to private keyfile.
   */
  public static Credentials getCredentialFromPrivateKeyServiceAccount(
      String serviceAccountEmail, String privateKeyFile)
      throws IOException, GeneralSecurityException {
    return getCredentialFromPrivateKeyServiceAccount(
        serviceAccountEmail, privateKeyFile, CLOUD_BIGTABLE_ALL_SCOPES);
  }

  /**
   * Initializes OAuth2 credential from a private keyfile, as described in
   * <a href="https://code.google.com/p/google-api-java-client/wiki/OAuth2#Service_Accounts"
   * > OAuth2 Service Accounts</a>.
   *
   * @param serviceAccountEmail Email address of the service account associated with the keyfile.
   * @param privateKeyFile Full local path to private keyfile.
   * @param scopes List of well-formed desired scopes to use with the credential.
   */
  public static Credentials getCredentialFromPrivateKeyServiceAccount(
      String serviceAccountEmail, String privateKeyFile, List<String> scopes)
      throws IOException, GeneralSecurityException {
    String clientId = null;
    String privateKeyId = null;
    PrivateKey privateKey =
        SecurityUtils.loadPrivateKeyFromKeyStore(SecurityUtils.getPkcs12KeyStore(),
          new FileInputStream(privateKeyFile), "notasecret", "privatekey", "notasecret");
    return new ServiceAccountCredentials(clientId, serviceAccountEmail, privateKey, privateKeyId,
        scopes, getHttpTransport());
  }

  /**
   * Initializes OAuth2 application default credentials based on the environment the code is running
   * in. If a service account is to be used with JSON file, set the environment variable with name
   * "GOOGLE_APPLICATION_CREDENTIALS" to the JSON file path. For more details on application default
   * credentials:
   * <a href="https://developers.google.com/accounts/docs/application-default-credentials" >
   * Application Default Credentials</a>.
   */
  public static Credentials getApplicationDefaultCredential() throws IOException,
      GeneralSecurityException {
    return GoogleCredentials.getApplicationDefault(getHttpTransport()).createScoped(
      CLOUD_BIGTABLE_ALL_SCOPES);
  }

  /**
   * Initializes OAuth2 application default credentials based on an inputStream.
   */
  public static Credentials getInputStreamCredential(InputStream inputStream) throws IOException,
      GeneralSecurityException {
    return GoogleCredentials.fromStream(inputStream, getHttpTransport()).createScoped(
      CLOUD_BIGTABLE_ALL_SCOPES);
  }
}
