/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;


/**
 *
 * @author nuwansa
 */
public class CloudRouterDescriptor {
  private final String routerType;
  private final String loadBalancer;

  public CloudRouterDescriptor(String routerType, String loadBalancer) {
    this.routerType = routerType;
    this.loadBalancer = loadBalancer;
  }

  public Class<? extends CloudRouter> getRouterType() {
    return CloudUtil.classForName(routerType);
  }

  public String getLoadBalancer() {
    return loadBalancer;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Class<? extends CloudRouter> routerType;
    private String loadBalancer;

    public Builder withRouterType(Class<? extends CloudRouter> routerType) {
      this.routerType = routerType;
      return this;
    }

    public Builder withLoadBalancer(String loadBalancer) {
      this.loadBalancer = loadBalancer;
      return this;
    }

    public CloudRouterDescriptor build() {
      return new CloudRouterDescriptor(routerType.getName(), loadBalancer);
    }
  }
}
