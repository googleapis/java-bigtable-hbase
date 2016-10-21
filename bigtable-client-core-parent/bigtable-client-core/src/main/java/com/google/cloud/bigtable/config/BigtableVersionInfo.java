package com.google.cloud.bigtable.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BigtableVersionInfo {

  private static final Logger LOG = new Logger(BigtableVersionInfo.class);

  public static final String CLIENT_VERSION = getVersion();
  public static final String JDK_VERSION = getJavaVersion();

  public static final String CORE_UESR_AGENT = "bigtable-" + CLIENT_VERSION +",jdk-" + JDK_VERSION;

  /**
   * Gets user agent from bigtable-hbase.properties. Returns a default dev user agent with current
   * timestamp if not found.
   */
  private static String getVersion() {
    final String defaultVersion = "dev-" + System.currentTimeMillis();
    final String fileName = "bigtable-version.properties";
    final String versionProperty = "bigtable.version";
    try (InputStream stream =
        BigtableVersionInfo.class.getResourceAsStream(fileName)) {
      if (stream == null) {
        LOG.error("Could not load properties file bigtable-version.properties");
        return defaultVersion;
      }

      Properties properties = new Properties();
      properties.load(stream);
      String value = properties.getProperty(versionProperty);
      if (value == null) {
        LOG.error("$1 not found in $2.", versionProperty, fileName);
      } else if (value.startsWith("$")){
        LOG.info("$1 property is not replaced.", versionProperty);
      } else {
        return value;
      }
    } catch (IOException e) {
      LOG.error("Error while trying to get user agent name from $1", e, fileName);
    }
    return defaultVersion;
  }

  /**
   * @return The java specification version; for example, 1.7 or 1.8.
   */
  private static String getJavaVersion() {
    return System.getProperty("java.specification.version");
  }
}
