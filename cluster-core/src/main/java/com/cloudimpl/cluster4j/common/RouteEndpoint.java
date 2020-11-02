/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

import java.util.Objects;

/**
 *
 * @author nuwansa
 */
public class RouteEndpoint {
  private final String host;
  private final int port;

  public RouteEndpoint(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public static RouteEndpoint create(String host, int port) {
    return new RouteEndpoint(host, port);
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 43 * hash + Objects.hashCode(this.host);
    hash = 43 * hash + this.port;
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final RouteEndpoint other = (RouteEndpoint) obj;
    if (this.port != other.port) {
      return false;
    }
    return Objects.equals(this.host, other.host);
  }

    @Override
    public String toString() {
        return "RouteEndpoint{" + "host=" + host + ", port=" + port + '}';
    }


  
}
