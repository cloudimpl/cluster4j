/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core.lb;

/**
 *
 * @author nuwansa
 */
public class LBRequest {
  private final String topic;
  private final String key;

  public LBRequest(String topic, String key) {
    this.topic = topic;
    this.key = key;
  }

  public String getTopic() {
    return topic;
  }

  public String getKey() {
    return key;
  }



}
