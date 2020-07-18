/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core.lb;

/**
 *
 * @author nuwansa
 */
public class LBResponse {
  private final String topic;
  private final String key;
  private final String id;

  public LBResponse(String topic, String id, String key) {
    this.topic = topic;
    this.id = id;
    this.key = key;
  }

  public String getTopic() {
    return topic;
  }

  public String getId() {
    return id;
  }

  public String getKey() {
    return key;
  }



}
