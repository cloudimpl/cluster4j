/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public interface EndpointListener {

  default Mono<Void> fireAndForget(Mono<CloudMessage> msg) {
    return Mono.error(new UnsupportedOperationException("Fire and forget not implemented."));
  }

  default Mono<CloudMessage> requestResponse(Mono<CloudMessage> msg) {
    return Mono.error(new UnsupportedOperationException("Request-Response not implemented."));
  }

  default Flux<CloudMessage> requestStream(Mono<CloudMessage> msg) {
    return Flux.error(new UnsupportedOperationException("Request-Stream not implemented."));
  }

  default Flux<CloudMessage> requestChannel(Publisher<CloudMessage> publisher) {
    return Flux.error(new UnsupportedOperationException("Request-Channel not implemented."));
  }

}
