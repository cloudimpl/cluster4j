/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.coreImpl;

import com.cloudimpl.cluster.common.FluxMap;
import com.cloudimpl.cluster.common.FluxStream;
import com.cloudimpl.cluster4j.core.CloudService;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import java.util.stream.Stream;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class CloudServiceRegistry {
  private final FluxMap<String, CloudService> services = new FluxMap<>();
  private final FluxMap<String, LocalCloudService> localServices = new FluxMap<>();
  private final ILogger logger;

  public CloudServiceRegistry(ILogger logger) {
    this.logger = logger.createSubLogger(CloudServiceRegistry.class);
  }


  public void register(CloudService service) {
    CloudService old = services.putIfAbsent(service.id(), service);
    if (old != null)
      throw new ServiceRegistryException(
          "duplicate service id " + old.id() + ", old = " + old.name() + ",new = " + service.name());
    try {
      service.init();
    } catch (Exception ex) {
        logger.exception(ex,"error registering service {0}", service.getDescriptor());
      services.remove(service.id());
    }
    if (service instanceof LocalCloudService)
      localServices.put(service.id(), (LocalCloudService) service);
  }

  public void unregister(String id) {
    CloudService service = services.remove(id);
    logger.info("service unregister id = {0} -> {1}", id, service);
    if (service != null && service instanceof LocalCloudService) {
      localServices.remove(id);
    }
  }

  public void unregisterByMemberId(String memberId) {
    logger.info("unregister by memberId {0}", memberId);
    services().filter(srv -> srv.memberId().equals(memberId)).forEach(srv -> unregister(srv.id()));
  }

  public Flux<FluxStream.Event<String, CloudService>> flux() {
    return services.flux();
  }

  public Flux<FluxStream.Event<String, LocalCloudService>> localFlux() {
    return localServices.flux();
  }

  public Stream<CloudService> services() {
    return services.values().stream();
  }

  public CloudService findLocalByName(String name)
  {
      return localServices.values().stream().filter(s->s.name().equals(name)).findFirst().orElse(null);
  }
  
  public CloudService findLocal(String id) {
    CloudService service = localServices.get(id);
    if (service == null)
      throw new ServiceRegistryException("local service with id " + id + " not found");
    return service;
  }

  public CloudService findService(String id) {
    CloudService service = services.get(id);
    if (service == null)
      throw new ServiceRegistryException("service with id " + id + " not found");
    return service;
  }
}
