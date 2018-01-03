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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wrapper around {@link org.apache.commons.logging.Log} to conditionally format
 * messages if a specified log level is enabled.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class Logger {
  protected final Log log;

  /**
   * <p>Constructor for Logger.</p>
   *
   * @param logClass a {@link java.lang.Class} object.
   */
  public Logger(Class<?> logClass) {
    this.log = LogFactory.getLog(logClass);
  }

  /**
   * <p>trace</p>
   *
   * @see Log#trace
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void trace(String message, Object ... args) {
    if (log.isTraceEnabled()) {
      log.trace(String.format(message, args));
    }
  }

  /**
   * <p>debug</p>
   *
   * @see Log#debug
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void debug(String message, Object ... args) {
    if (log.isDebugEnabled()) {
      log.debug(String.format(message, args));
    }
  }

  /**
   * <p>debug</p>
   *
   * @see Log#debug
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void debug(String message, Throwable t, Object ... args) {
    if (log.isDebugEnabled()) {
      log.debug(String.format(message, args), t);
    }
  }

  /**
   * <p>info</p>
   *
   * @see Log#info
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void info(String message, Object ... args) {
    if (log.isInfoEnabled()) {
      log.info(String.format(message, args));
    }
  }

  /**
   * <p>info</p>
   *
   * @see Log#info
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void info(String message, Throwable t, Object ... args) {
    if (log.isInfoEnabled()) {
      log.info(String.format(message, args), t);
    }
  }

  /**
   * <p>warn</p>
   *
   * @see Log#warn
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void warn(String message, Object ... args) {
    if (log.isWarnEnabled()) {
      log.warn(String.format(message, args));
    }
  }

  /**
   * <p>warn</p>
   *
   * @see Log#warn
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void warn(String message, Throwable t, Object ... args) {
    if (log.isWarnEnabled()) {
      log.warn(String.format(message, args), t);
    }
  }

  /**
   * <p>error</p>
   *
   * @see Log#error
   * @param message Format string.
   * @param args Arguments for format string.
   */
  public void error(String message, Object ... args) {
    if (log.isErrorEnabled()) {
      log.error(String.format(message, args));
    }
  }

  /**
   * <p>error</p>
   *
   * @see Log#error
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void error(String message, Throwable t, Object ... args) {
    if (log.isErrorEnabled()) {
      log.error(String.format(message, args), t);
    }
  }

  /**
   * <p>fatal</p>
   *
   * @see Log#fatal
   * @param message a {@link java.lang.String} object.
   * @param args a {@link java.lang.Object} object.
   */
  public void fatal(String message, Object ... args) {
    if (log.isFatalEnabled()) {
      log.fatal(String.format(message, args));
    }
  }

  /**
   * <p>fatal</p>
   *
   * @see Log#fatal
   * @param message Format string.
   * @param t a {@link java.lang.Throwable} object.
   * @param args Arguments for format string.
   */
  public void fatal(String message, Throwable t, Object ... args) {
    if (log.isFatalEnabled()) {
      log.fatal(String.format(message, args), t);
    }
  }
}
