/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.core;

import com.cloudimpl.cluster4j.common.CloudMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public interface CloudService extends Comparable<CloudService> {

  void init();

  String id();

  String nodeId();

  String memberId();
  
  String name();

  CloudServiceDescriptor getDescriptor();

  <T> Mono<T> requestReply(CloudMessage msg);

  <T> Flux<T> requestStream(CloudMessage msg);

  <T> Mono<Void> send(CloudMessage msg);

  default boolean isLocal() {return false;}
  
  @Override
  default int compareTo(CloudService other) {
    return id().compareTo(other.id());
  }
}
