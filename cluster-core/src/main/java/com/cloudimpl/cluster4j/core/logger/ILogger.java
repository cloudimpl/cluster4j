/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core.logger;

import java.util.Collection;

/**
 *
 * @author nuwansa
 */
public interface ILogger {

  /**
   * Create new sub logger from the current logger.
   *
   * @param group the new group to be set
   * @param section the new section to be set
   * @return the sub logger
   */
  ILogger createSubLogger(String group, String section);

  /**
   * Create new sub logger from the current logger.
   *
   * @param cls input class for the logger
   * @return the sub logger
   */
  ILogger createSubLogger(Class<?> cls);

  /**
   * Log a debug message. This will be added to the log file as "DEBUG" line. Debug messages are logged when application
   * needs to write a debug information line. This will not be popped up as an alert.
   *
   * @param format the message format
   * @param args arguments
   */
  void debug(String format, Object... args);

  /**
   * Log an unexpected behavior but cannot be recovered automatically (i.e. an error). This will be added to the log
   * file as "ERROR" line. Error messages are logged when application needs to write an error line which need to be
   * notify to administrators. This will be popped up as an alert.
   *
   * @param format the message format
   * @param args arguments
   */
  void error(String format, Object... args);

  /**
   * Log an java {@link Exception} (or {@link Throwable}). This will be added to the log file as "EXCEPTION" line.
   * Exception messages are logged when application needs to write an exception line which need to be notify to
   * administrators. This will be popped up as an alert.
   *
   * @param exception the message format
   * @param format the message format
   * @param args arguments
   */
  void exception(Throwable exception, String format, Object... args);

  /**
   *
   * @return all the loggers know by this node
   */
  Collection<ILogger> getAlLoggers();

  /**
   * Gets the current log level of the {@link Logger}.
   *
   * @return the log level
   */
  LogLevel getLevel();

  /**
   * Log an information message. This will be added to the log file as "INFO" line. Information messages are logged when
   * application needs to write an information line. This will not be popped up as an alert.
   *
   * @param format the message format
   * @param args arguments
   */
  void info(String format, Object... args);

  /**
   * Gets the debug enable state of the {@link Logger}.
   *
   * @return the debug enable state
   */
  boolean isDebugEnabled();

  /**
   * Gets the error enable state of the {@link Logger}.
   *
   * @return the error enable state
   */
  boolean isErrorEnabled();

  /**
   * Gets the exception enable state of the {@link Logger}.
   *
   * @return the exception enable state
   */
  boolean isExceptionEnabled();

  /**
   * Gets the exception enable state of the {@link Logger}.
   *
   * @return the exception enable state
   */
  boolean isInfoEnabled();

  /**
   * Gets the exception enable state of the {@link Logger}.
   *
   * @return the exception enable state
   */
  boolean isNoticeEnabled();

  /**
   * Gets the verbose enable state of the {@link Logger}.
   *
   * @return the verbose enable state
   */
  boolean isVerboseEnabled();

  /**
   * Gets the warning enable state of the {@link Logger}.
   *
   * @return the warning enable state
   */
  boolean isWarningEnabled();

  /**
   * Log an notification message. This will be added to the log file as "NOTICE" line. Notification messages are logged
   * when application needs to write an information line which need to be notify to administrators. This will be popped
   * up as an alert.
   *
   * @param format the message format
   * @param args arguments
   */
  void notice(String format, Object... args);

  /**
   * Sets the current log level of the {@link Logger}.
   *
   * @param level the log level to be set
   */
  void setLevel(LogLevel level);

  /**
   * Log more descriptive details. This will be added to the log file as "VERB" line. Verbose messages are logged when
   * application needs more descriptive information for debugging purpose. This will not be popped up as an alert.
   *
   * @param format the message format
   * @param args arguments
   */
  void verbose(String format, Object... args);

  /**
   * Log an unexpected behavior but recovered automatically (i.e. an warning). This will be added to the log file as
   * "WARN" line. Warning messages are logged when application needs to write an warning line which need to be notify to
   * administrators. This will be popped up as an alert.
   *
   * @param format the message format
   * @param args arguments
   */
  void warn(String format, Object... args);

  /**
   * The logging level.
   */
  public enum LogLevel {
    INHERIT,
    NONE,
    EXCEPTION,
    ERROR,
    WARNING,
    NOTICE,
    INFO,
    DEBUG,
    VERBOSE
  }
}
