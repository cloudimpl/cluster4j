/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.le;

/**
 *
 * @author nuwansa
 */
public class LeaderElectionException extends RuntimeException {

  public LeaderElectionException(String message) {
    super(message);
  }

}
