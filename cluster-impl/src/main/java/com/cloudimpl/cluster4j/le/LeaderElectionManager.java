/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.le;

import com.cloudimpl.cluster.common.FluxMap;
import com.cloudimpl.cluster.common.FluxStream;
import com.cloudimpl.cluster4j.le.LeaderElection.LeaderInfo;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class LeaderElectionManager {

  private final Map<String, LeaderElection> leaders = new ConcurrentHashMap<>();
  private final FluxMap<String, LeaderInfo> leaderMap = new FluxMap<>();

  public LeaderElection create(String leaderGroup, String memberId, Map<String, LeaderElection.LeaderInfo> dataStore,
      long leaderExpirePeriod,
      LeaderElection.Listener listener, ILogger logger) {
    return leaders.computeIfAbsent(leaderGroup,
        (name) -> new LeaderElection(leaderGroup, memberId, dataStore, leaderExpirePeriod, listener, leaderMap,
            logger));
  }

  public Optional<String> getLeaderId(String leaderGroup) {
    LeaderElection le = leaders.get(leaderGroup);
    if (le == null)
      return Optional.empty();
    return (le.getLeaderInfo().isPresent()) ? Optional.of(le.getLeaderInfo().get().getLeaderId()) : Optional.empty();
  }

  public Flux<FluxStream.Event<String, LeaderInfo>> flux() {
    return leaderMap.flux();
  }
}
