package com.google.cloud.bigtable.hbase;

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import org.apache.hadoop.conf.Configuration;

/**
 * Allows users to set an explicit {@link Credentials} object.
 *
 * <p>For internal use only - public for technical reasons.
 *
 * @see BigtableConfiguration#withCredentials(Configuration, Credentials).
 */
@InternalApi("For internal usage only")
public class BigtableExtendedConfiguration extends Configuration {
  private Credentials credentials;

  BigtableExtendedConfiguration(Configuration conf, Credentials credentials) {
    super(conf);
    this.credentials = credentials;
  }

  public Credentials getCredentials() {
    return credentials;
  }
}
