package com.google.cloud.bigtable.hbase;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.List;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.compute.ComputeCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.SecurityUtils;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
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
  private static synchronized HttpTransport getHttpTransport()
      throws IOException, GeneralSecurityException {
    if (httpTransport == null) {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    }
    return httpTransport;
  }

  /**
   * Initializes OAuth2 credential using preconfigured ServiceAccount settings on the local
   * GCE VM. See: <a href="https://developers.google.com/compute/docs/authentication"
   * >Authenticating from Google Compute Engine</a>.
   */
  public static Credentials getCredentialFromMetadataServiceAccount()
      throws IOException, GeneralSecurityException {
    Credentials cred = new ComputeEngineCredentials(getHttpTransport());
    try {
      cred.refresh();
    } catch (IOException e) {
      throw new IOException("Error getting access token from metadata server at: " +
          ComputeCredential.TOKEN_SERVER_ENCODED_URL, e);
    }
    return cred;
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
        scopes);
  }

  public static Credentials getApplicationDefaultCredential() throws IOException {
    return GoogleCredential.getApplicationDefault();
  }
}
