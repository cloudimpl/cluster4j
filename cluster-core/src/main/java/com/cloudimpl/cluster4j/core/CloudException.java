/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

/**
 *
 * @author nuwansa
 */
public class CloudException extends RuntimeException {

  public CloudException(Throwable thr) {
    super(thr);
  }

  public CloudException(String message) {
    super(message);
  }


}
