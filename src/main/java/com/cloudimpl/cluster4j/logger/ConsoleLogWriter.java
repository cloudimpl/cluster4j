package com.cloudimpl.cluster4j.logger;

/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */



/**
 * The implementation of the {@link LogWriter} which writes the log to console.
 */
public final class ConsoleLogWriter implements LogWriter {

  @Override
  public synchronized void writeLogLine(LogMessage message) {
    System.out.println(message);
  }
}
