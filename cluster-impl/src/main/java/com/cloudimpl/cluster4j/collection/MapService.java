/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.collection;

import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.Named;
import com.cloudimpl.cluster4j.le.LeaderElection;
import java.util.Map;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class MapService implements Function<MapRequest, Mono<MapResponse>>, LeaderElection.Listener {

  private final String serviceId;
  private final DataEngine engine;

  @Inject
  public MapService(@Named("@srvId") String serviceId, DataEngine engine) {
    this.serviceId = serviceId;
    this.engine = engine;
  }

  @Override
  public Mono<MapResponse> apply(MapRequest mapReq) {
    Map map = engine.getMap(mapReq.getKey());
    return null;
  }

  @Override
  public void leaderChange(LeaderElection le, String leaderId) {

  }

}
