package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.auth.Credentials;
import com.google.auth.RequestMetadataCallback;
import com.google.cloud.bigtable.hbase.BigtableOAuthCredentials;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * A wrapper that implements @{@link Credentials} interface and delegates calls to  the underlying
 * @{@link BigtableCredentials} object. This is required to decouple the users of shaded bigtable
 * from the @{@link Credentials} class.
 */
public class BigtableCredentialsWrapper extends Credentials {

  public BigtableCredentialsWrapper(BigtableOAuthCredentials btCredentials) {
    this.bigtableCredentials = btCredentials;
  }

  private BigtableOAuthCredentials bigtableCredentials;

  @Override
  public String getAuthenticationType() {
    return "OAuth2";
  }

  @Override
  public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
    try {
      return bigtableCredentials.getRequestMetadata(uri, MoreExecutors.directExecutor()).get();
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch credentials.", e);
    }
  }

  public void getRequestMetadata(final URI uri, Executor executor, final RequestMetadataCallback callback) {
    executor.execute(new Runnable() {
      public void run() {
        blockingGetToCallback(uri, callback);
      }
    });
  }

  @Override
  public boolean hasRequestMetadata() {
    return true;
  }

  @Override
  public boolean hasRequestMetadataOnly() {
    return true;
  }

  @Override
  public void refresh() throws IOException {
    // No-op. btCredentials should refresh internally as required.
  }

  public BigtableOAuthCredentials getBigtableCredentials() {
    return bigtableCredentials;
  }
}
