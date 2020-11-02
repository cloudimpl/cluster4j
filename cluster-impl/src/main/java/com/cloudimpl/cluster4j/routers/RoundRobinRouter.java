/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.routers;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudRouter;
import com.cloudimpl.cluster4j.core.CloudService;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.Named;
import com.cloudimpl.cluster4j.core.RouterException;
import com.cloudimpl.cluster4j.coreImpl.CloudServiceRegistry;
import com.cloudimpl.cluster.common.FluxStream;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class RoundRobinRouter implements CloudRouter {

  private Set<CloudService> services = new ConcurrentSkipListSet<>();
  private Iterator<CloudService> iterator;
  private final String topic;

  @Inject
  public RoundRobinRouter(@Named("@topic") String topic, CloudServiceRegistry serviceRegistry) {
    this.topic = topic;
    serviceRegistry.flux().filter(e -> e.getType() == FluxStream.Event.Type.ADD)
        .map(e -> e.getValue())
        .filter(srv -> srv.name().equals(topic))
        .doOnNext(srv -> services.add(srv))
        .subscribe();
    serviceRegistry.flux().filter(e -> e.getType() == FluxStream.Event.Type.REMOVE)
        .map(e -> e.getValue())
        .filter(srv -> srv.name().equals(topic))
        .doOnNext(srv -> services.remove(srv))
        .subscribe();
    iterator = services.iterator();
  }

  @Override
  public Mono<CloudService> route(CloudMessage msg) {
    if (!iterator.hasNext())
      iterator = services.iterator();

    if (iterator.hasNext())
      return Mono.just(iterator.next());
    else
      return Mono.error(new RouterException("service [" + topic + "] not found"));

  }

}
