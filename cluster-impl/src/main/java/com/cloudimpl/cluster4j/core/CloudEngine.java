/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public interface CloudEngine {

  String id();

  <T> Mono<T> requestReply(String topic, Object request);

  <T> Flux<T> requestStream(String topic, Object request);

  Mono<Void> send(String topic, Object data);

  CloudServiceRegistry getServiceRegistry();

  void registerService(String name, CloudFunction cloudFunc);
}
