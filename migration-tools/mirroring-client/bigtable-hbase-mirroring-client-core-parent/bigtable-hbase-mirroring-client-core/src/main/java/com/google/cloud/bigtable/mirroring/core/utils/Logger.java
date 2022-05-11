/*
 * Copyright 2015 Google LLC
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
// (mwalkiewicz): This file was copied from com.google.cloud.bigtable.hbase.Logger, we want to avoid
// having a dependency to that package, but this tool is super useful.
package com.google.cloud.bigtable.mirroring.core.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wrapper around {@link org.apache.commons.logging.Log} to conditionally format messages if a
 * specified log level is enabled.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class Logger {
  protected final Log log;

  /**
   * Constructor for Logger.
   *
   * @param logClass a {@link java.lang.Class} object.
   */
  public Logger(Class<?> logClass) {
    this.log = LogFactory.getLog(logClass);
  }

  /**
   * trace
   *
   * @see Log#trace
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void trace(String message, Object... args) {
    if (log.isTraceEnabled()) {
      log.trace(String.format(message, args));
    }
  }

  /**
   * debug
   *
   * @see Log#debug
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void debug(String message, Object... args) {
    if (log.isDebugEnabled()) {
      log.debug(String.format(message, args));
    }
  }

  /**
   * debug
   *
   * @see Log#debug
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void debug(String message, Throwable t, Object... args) {
    if (log.isDebugEnabled()) {
      log.debug(String.format(message, args), t);
    }
  }

  /**
   * info
   *
   * @see Log#info
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void info(String message, Object... args) {
    if (log.isInfoEnabled()) {
      log.info(String.format(message, args));
    }
  }

  /**
   * info
   *
   * @see Log#info
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void info(String message, Throwable t, Object... args) {
    if (log.isInfoEnabled()) {
      log.info(String.format(message, args), t);
    }
  }

  /**
   * warn
   *
   * @see Log#warn
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void warn(String message, Object... args) {
    if (log.isWarnEnabled()) {
      log.warn(String.format(message, args));
    }
  }

  /**
   * warn
   *
   * @see Log#warn
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void warn(String message, Throwable t, Object... args) {
    if (log.isWarnEnabled()) {
      log.warn(String.format(message, args), t);
    }
  }

  /**
   * error
   *
   * @see Log#error
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void error(String message, Object... args) {
    if (log.isErrorEnabled()) {
      log.error(String.format(message, args));
    }
  }

  /**
   * error
   *
   * @see Log#error
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void error(String message, Throwable t, Object... args) {
    if (log.isErrorEnabled()) {
      log.error(String.format(message, args), t);
    }
  }

  /**
   * fatal
   *
   * @see Log#fatal
   * @param message a {@link java.lang.String} object.
   * @param args a {@link java.lang.Object} object.
   */
  public void fatal(String message, Object... args) {
    if (log.isFatalEnabled()) {
      log.fatal(String.format(message, args));
    }
  }

  /**
   * fatal
   *
   * @see Log#fatal
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void fatal(String message, Throwable t, Object... args) {
    if (log.isFatalEnabled()) {
      log.fatal(String.format(message, args), t);
    }
  }
}
