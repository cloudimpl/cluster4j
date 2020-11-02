/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

import java.net.InetSocketAddress;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public interface EndpointListener<T> {

  default void onInit(InetSocketAddress addr) {};
  
  default Mono<Void> fireAndForget(T msg) {
    return Mono.error(new UnsupportedOperationException("Fire and forget not implemented."));
  }

  default Mono<T> requestResponse(T msg) {
    return Mono.error(new UnsupportedOperationException("Request-Response not implemented."));
  }

  default Flux<T> requestStream(T msg) {
    return Flux.error(new UnsupportedOperationException("Request-Stream not implemented."));
  }

  default Flux<T> requestChannel(Publisher<T> publisher) {
    return Flux.error(new UnsupportedOperationException("Request-Channel not implemented."));
  }

}
