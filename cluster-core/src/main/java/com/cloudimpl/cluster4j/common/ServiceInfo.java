/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

/**
 *
 * @author nuwansa
 */
public class ServiceInfo {
  private final String serviceName;
  private final ClientRouterDescriptor routerDesc;

  public ServiceInfo(String serviceName, ClientRouterDescriptor routerDesc) {
    this.serviceName = serviceName;
    this.routerDesc = routerDesc;
  }

  public String getServiceName() {
    return serviceName;
  }

  public ClientRouterDescriptor getRouterDesc() {
    return routerDesc;
  }

}
