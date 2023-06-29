package com.google.cloud.bigtable.hbase.wrappers.veneer;

import com.google.auth.Credentials;
import com.google.auth.RequestMetadataCallback;
import com.google.cloud.bigtable.hbase.BigtableCredentials;
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

  public BigtableCredentialsWrapper(BigtableCredentials btCredentials) {
    this.bigtableCredentials = btCredentials;
  }

  private BigtableCredentials bigtableCredentials;

  @Override
  public String getAuthenticationType() {
    throw new UnsupportedOperationException("This method is not supported yet.");
  }

  @Override
  public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
    return bigtableCredentials.getRequestMetadata(uri);
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
    throw new UnsupportedOperationException("This method is not supported yet.");
  }

  @Override
  public boolean hasRequestMetadataOnly() {
    throw new UnsupportedOperationException("This method is not supported yet.");
  }

  @Override
  public void refresh() throws IOException {
    // No-op. btCredentials should refresh internally as required.
  }

  public BigtableCredentials getBigtableCredentials() {
    return bigtableCredentials;
  }
}
