package com.google.cloud.bigtable.hbase;

import com.google.api.core.InternalExtensionOnly;
import com.google.auth.Credentials;
import org.apache.hadoop.conf.Configuration;

/**
 * Allows users to set an explicit {@link Credentials} object.
 *
 * @see BigtableConfiguration#withCredentials(Configuration, Credentials).
 */
@InternalExtensionOnly
class BigtableExtendedConfiguration extends Configuration {
  private Credentials credentials;

  BigtableExtendedConfiguration(Configuration conf, Credentials credentials) {
    super(conf);
    this.credentials = credentials;
  }

  public Credentials getCredentials() {
    return credentials;
  }
}
