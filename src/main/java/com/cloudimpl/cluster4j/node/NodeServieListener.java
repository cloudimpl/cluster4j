/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.node;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.common.EndpointListener;
import com.cloudimpl.cluster4j.core.CloudMsgHdr;
import com.cloudimpl.cluster4j.core.CloudService;
import com.cloudimpl.cluster4j.core.CloudServiceRegistry;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class NodeServieListener implements EndpointListener {
  private final CloudServiceRegistry registry;

  public NodeServieListener(CloudServiceRegistry registry) {
    this.registry = registry;
  }

  @Override
  public Mono<Void> fireAndForget(Mono<CloudMessage> msg) {
    return msg.flatMap(m -> {
      String serviceId = m.attr(CloudMsgHdr.SERVICE_ID);
      if (serviceId == null)
        return Mono.error(new ServiceException("service id not found to route"));
      CloudService service = registry.findLocal(serviceId);
      return service.send(m);
    });

  }

  @Override
  public Mono<CloudMessage> requestResponse(Mono<CloudMessage> msg) {
    return msg.flatMap(m -> {
      String serviceId = m.attr(CloudMsgHdr.SERVICE_ID);
      if (serviceId == null)
        return Mono.error(new ServiceException("service id not found to route"));
      CloudService service = registry.findLocal(serviceId);
      return service.requestReply(m);
    });
  }

  @Override
  public Flux<CloudMessage> requestStream(Mono<CloudMessage> msg) {
    return msg.flatMapMany(m -> {
      String serviceId = m.attr(CloudMsgHdr.SERVICE_ID);
      if (serviceId == null)
        return Mono.error(new ServiceException("service id not found to route"));
      CloudService service = registry.findLocal(serviceId);
      return service.requestStream(m);
    });
  }

}
