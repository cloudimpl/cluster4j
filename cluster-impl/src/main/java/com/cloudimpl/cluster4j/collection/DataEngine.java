/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.collection;

import com.cloudimpl.cluster4j.coreImpl.CloudServiceRegistry;
import com.cloudimpl.cluster4j.coreImpl.FluxMap;
import com.cloudimpl.cluster4j.le.LeaderElectionManager;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author nuwansa
 */
public class DataEngine {

  private final LeaderElectionManager leaderManager;
  private final Map<String, FluxMap> maps = new ConcurrentHashMap<>();
  private final ILogger logger;
  private final CloudServiceRegistry serviceRegistry;
  private final AtomicReference<String> serviceRef = new AtomicReference<>();

  public DataEngine(CloudServiceRegistry serviceRegistry, LeaderElectionManager leaderManager, ILogger logger) {
    this.serviceRegistry = serviceRegistry;
    this.leaderManager = leaderManager;
    this.logger = logger;
    this.serviceRegistry.localFlux().filter(p -> p.getValue().name().equals(MapService.class.getName()))
        .subscribe(s -> serviceRef.set(s.getValue().id()));
  }

  // public <K, V> Map<K, V> createMap(Class<K> keyType, Class<V> valueType, String name) {
  // String key = String.join(":", keyType.getName(), valueType.getName(), name);
  // return maps.computeIfAbsent(key, k -> createDistMap(k));
  // }

  public <K, V> Map<K, V> getMap(Class<K> keyType, Class<V> valueType, String name) {
    String key = String.join(":", keyType.getName(), valueType.getName(), name);
    return getMap(key);
  }

  public <K, V> Map<K, V> getMap(String key) {
    return maps.get(key);
  }

  // private <K, V> FluxMap createDistMap(String key) {
  // Map<K, V> map = new FluxMap();
  // LeaderElection el = this.leaderManager.create(key, serviceRef.get(), createDataStore(key), 10000, map, logger);
  // serviceRegistry.flux()
  // .filter(e -> e.getType() == FluxStream.Event.Type.ADD || e.getType() == FluxStream.Event.Type.UPDATE)
  // .subscribe(e -> el.addMember(e.getValue().id()));
  // serviceRegistry.flux().filter(e -> e.getType() == FluxStream.Event.Type.REMOVE)
  // .subscribe(e -> el.removeMember(e.getValue().id()));
  // el.run();
  // return map;
  // }

  public <K, V> Map<K, V> createDataStore(String name) {
    return new ConcurrentHashMap<>();
  }

}
