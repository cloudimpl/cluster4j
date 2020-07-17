/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

/**
 *
 * @author nuwansa
 */
public class SharedResources {
  private final Injector injector;

  public SharedResources(Injector injector) {
    this.injector = injector;
  }

  public <T> void register(Class<T> clsType, T instance) {
    this.injector.bind(clsType).to(instance);
  }
}
