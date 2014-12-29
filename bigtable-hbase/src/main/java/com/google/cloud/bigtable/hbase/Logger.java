package com.google.cloud.bigtable.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wrapper around commons Log to conditionally format messages if a specified log level is enabled.
 */
public class Logger {
  protected final Log log;

  public Logger(Class<?> logClass) {
    this.log = LogFactory.getLog(logClass);
  }

  public void trace(String message, Object ... args) {
    if (log.isTraceEnabled()) {
      log.trace(String.format(message, args));
    }
  }

  public void debug(String message, Object ... args) {
    if (log.isDebugEnabled()) {
      log.debug(String.format(message, args));
    }
  }

  public void info(String message, Object ... args) {
    if (log.isInfoEnabled()) {
      log.info(String.format(message, args));
    }
  }

  public void warn(String message, Object ... args) {
    if (log.isWarnEnabled()) {
      log.warn(String.format(message, args));
    }
  }

  public void error(String message, Object ... args) {
    if (log.isErrorEnabled()) {
      log.error(String.format(message, args));
    }
  }

  public void fatal(String message, Object ... args) {
    if (log.isFatalEnabled()) {
      log.fatal(String.format(message, args));
    }
  }
}
