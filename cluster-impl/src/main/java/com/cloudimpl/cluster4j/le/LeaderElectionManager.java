/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.le;

import com.cloudimpl.cluster.collection.CollectionOptions;
import com.cloudimpl.cluster.collection.CollectionProvider;
import com.cloudimpl.cluster.common.FluxMap;
import com.cloudimpl.cluster.common.FluxStream;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.Named;
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

    private final CollectionProvider collectionProvider;
    private final CollectionOptions options;
    @Inject
    public LeaderElectionManager(CollectionProvider collectionProvider,@Named("leaderOptions") CollectionOptions options) {
        this.collectionProvider = collectionProvider;
        this.options = options;
    }

    public LeaderElection create(String leaderGroup, String memberId,
            long leaderExpirePeriod,
            ILogger logger) {
        return leaders.computeIfAbsent(leaderGroup+"#"+memberId,
                (name) -> new LeaderElection(leaderGroup, memberId, this.collectionProvider.createHashMap("leaderGroup#"+leaderGroup, options), leaderExpirePeriod, leaderMap,
                        logger));
    }

    public Flux<FluxStream.Event<String, LeaderInfo>> flux() {
        return leaderMap.flux();
    }
}
