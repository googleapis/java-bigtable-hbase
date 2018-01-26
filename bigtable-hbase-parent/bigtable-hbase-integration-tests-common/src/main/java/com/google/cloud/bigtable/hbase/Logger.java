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

  public void warn(String message, Throwable t, Object ... args) {
    if (log.isWarnEnabled()) {
      log.warn(String.format(message, args), t);
    }
  }

  public void error(String message, Object ... args) {
    if (log.isErrorEnabled()) {
      log.error(String.format(message, args));
    }
  }

  public void error(String message, Throwable t, Object ... args) {
    if (log.isErrorEnabled()) {
      log.error(String.format(message, args), t);
    }
  }

  public void fatal(String message, Object ... args) {
    if (log.isFatalEnabled()) {
      log.fatal(String.format(message, args));
    }
  }

  public void fatal(String message, Throwable t, Object ... args) {
    if (log.isFatalEnabled()) {
      log.fatal(String.format(message, args), t);
    }
  }
}
