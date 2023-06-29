package com.google.cloud.bigtable.hbase;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * A public interface that should be implemented by the users who want to inject a custom
 * {@link com.google.auth.Credentials} implementation for auth purposes. Clients can't directly
 * override the {@link com.google.auth.Credentials} class as its shaded by Cloud Bigtable client.
 *
 * Hence, customers should implement this class, which will be used for authentication.
 */
public abstract class BigtableCredentials {

  /**
   * All subclasses must implement this constructor and populate the @configuration.
   *
   * @param configuration The HBase configuration
   */
  public BigtableCredentials(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Get the current request metadata.
   *
   * <p>This should be called by the transport layer on each request, and the data should be
   * populated in headers or other context.
   *
   * <p>The convention for handling binary data is for the key in the returned map to end with
   * {@code "-bin"} and for the corresponding values to be base64 encoded.
   *
   * <p>This class should handle caching and refeshing of the metadata associated with the request.
   *
   * @param uri URI of the entry point for the request.
   */
  public abstract Map<String, List<String>> getRequestMetadata(URI uri) throws IOException;

  public Configuration getConfiguration() {
    return configuration;
  }

  protected Configuration configuration;
}
