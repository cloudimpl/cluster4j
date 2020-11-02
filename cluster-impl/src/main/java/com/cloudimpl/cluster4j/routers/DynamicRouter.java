/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.routers;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudRouter;
import com.cloudimpl.cluster4j.core.CloudRouterDescriptor;
import com.cloudimpl.cluster4j.core.CloudService;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.Named;
import com.cloudimpl.cluster4j.core.RouterException;
import com.cloudimpl.cluster4j.core.lb.LBRequest;
import com.cloudimpl.cluster4j.core.lb.LBResponse;
import com.cloudimpl.cluster4j.coreImpl.CloudServiceRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class DynamicRouter implements CloudRouter {

  private final String topic;
  private final CloudRouterDescriptor desc;
  private final Map<String, CloudService> mapServices = new ConcurrentHashMap<>();
  private final BiFunction<String, Object, Mono> rrHnd;
  private final CloudServiceRegistry registry;

  @Inject
  public DynamicRouter(@Named("@topic") String topic, CloudServiceRegistry serviceRegistry,
      CloudRouterDescriptor desc, @Named("RRHnd") BiFunction<String, Object, Mono> rrHnd) {
    this.topic = topic;
    this.desc = desc;
    this.rrHnd = rrHnd;
    this.registry = serviceRegistry;
  }


  @Override
  public Mono<CloudService> route(CloudMessage msg) {
      
    if (msg.getKey() == null)
      return Mono.error(new RouterException("key not found to route for service [" + topic + "]"));
    return find(msg.getKey());
  }


  private Mono<CloudService> find(String key) {
    CloudService service = mapServices.get(key);
    if (service != null)
      return Mono.just(service);
    return rrHnd.apply(this.desc.getLoadBalancer(), new LBRequest(topic, key))
        .flatMap(r -> updateMap((LBResponse) r));
  }

  private Mono<CloudService> updateMap(LBResponse resp) {
    CloudService service = registry.findService(resp.getId());
    if (service == null)
      return Mono.error(new RouterException("service [" + topic + "] not found"));
    mapServices.put(resp.getKey(), service);
    return Mono.just(service);
  }
}
