/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author nuwansa
 */
public class CloudMessage {
  private final Object data;
  protected final Map<String, String> meta;
  private final String key;

  public CloudMessage(Object data, String key) {
    this.data = data;
    this.meta = new HashMap<>();
    this.key = key;
  }

  public CloudMessage(Object data, String key, Map<String, String> meta) {
    this.data = data;
    this.meta = Collections.unmodifiableMap(meta);
    this.key = key;
  }

  public <T> T data() {
    return (T) data;
  }

  public String getKey() {
    return key;
  }

  public CloudMessage withAttr(String attr, String value) {
    return CloudMessage.builder().from(this).withAttr(attr, value).build();
  }

  public String attr(String attr) {
    return meta.get(attr);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Object data;
    private Map<String, String> meta = new HashMap<>();
    private String key;

    public Builder() {}

    public Builder from(CloudMessage msg) {
      this.data = msg.data;
      this.meta = new HashMap<>(msg.meta);
      this.key = msg.getKey();
      return this;
    }

    public Builder withData(Object data) {
      this.data = data;
      return this;
    }

    public Builder withKey(String key) {
      this.key = key;
      return this;
    }

    public Builder withAttr(String name, String value) {
      this.meta.put(name, value);
      return this;
    }

    public CloudMessage build() {
      return new CloudMessage(data, key, meta);
    }

  }

}
