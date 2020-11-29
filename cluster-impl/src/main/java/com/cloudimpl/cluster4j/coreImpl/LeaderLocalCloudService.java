/*
 * Copyright 2020 nuwansa.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudimpl.cluster4j.coreImpl;

import com.cloudimpl.cluster.common.FluxStream;
import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudService;
import com.cloudimpl.cluster4j.core.CloudServiceDescriptor;
import com.cloudimpl.cluster4j.core.CloudUtil;
import com.cloudimpl.cluster4j.core.Injector;
import com.cloudimpl.cluster4j.core.LeaderLifeCycle;
import com.cloudimpl.cluster4j.core.Named;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import com.cloudimpl.cluster4j.le.LeaderElection;
import com.cloudimpl.cluster4j.le.LeaderElectionManager;
import com.cloudimpl.cluster4j.le.LeaderInfoRequest;
import com.cloudimpl.cluster4j.le.LeaderInfoResponse;
import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import reactor.core.publisher.Flux;
import reactor.retry.Retry;

/**
 *
 * @author nuwansa
 */
public class LeaderLocalCloudService extends LocalCloudService {

    private final LeaderElectionManager elMan;
    private final ILogger logger;
    private LeaderElection el;
    private final CloudServiceRegistry reg;
    private final BiFunction<String, CloudMessage, Flux<LeaderInfoResponse>> rsHnd;

    public LeaderLocalCloudService(Supplier<String> memberIdProvider, String nodeId, Injector injector,
            CloudServiceDescriptor descriptor, LeaderElectionManager elMan, ILogger logger, CloudServiceRegistry reg, BiFunction<String, CloudMessage, Flux<LeaderInfoResponse>> rsHnd) {
        super(memberIdProvider, nodeId, injector, descriptor);
        this.reg = reg;
        this.elMan = elMan;
        this.logger = logger;
        this.rsHnd = rsHnd;
    }

    @Override
    public void init() {
        this.el = this.elMan.create(this.name, this.id, 10000, logger);
        Injector inject = injector.with("@srvId", id).with("@srvName", name)
                .with(LeaderElection.class, el);
        this.function = CloudUtil.newInstance(inject, descriptor.getFunctionType());
        if (this.function instanceof LeaderLifeCycle) {
            LeaderLifeCycle leaderListner = (LeaderLifeCycle) this.function;
            el.run((LeaderElection e, String leaderId) -> leaderListner.onLeaderElect(leaderId));
        } else {
            el.run((LeaderElection e, String leaderId) -> {
            });
        }
        watchOthers();
    }

    private void watchOthers() {
        reg.flux().filter(e -> e.getType() == FluxStream.Event.Type.ADD | e.getType() == FluxStream.Event.Type.UPDATE)
                .map(e -> e.getValue())
                .filter(s -> s.name().equals(this.name))
                .filter(s -> s != this).doOnNext(s -> this.el.addMember(s.id())).subscribe();
        reg.flux().filter(e -> e.getType() == FluxStream.Event.Type.REMOVE)
                .map(e -> e.getValue())
                .filter(s -> s.name().equals(this.name))
                .filter(s -> s != this).doOnNext(s -> this.el.removeMember(s.id())).subscribe();

        reg.flux().filter(e -> e.getType() == FluxStream.Event.Type.ADD)
                .map(e -> e.getValue())
                .filter(s -> s.name().equals(this.name))
                .filter(s -> s != this).flatMap(s -> connectToInfoEndpoint(s))
                .doOnNext(resp -> el.addLeaderInfo(resp.getLeaderInfo()))
                .subscribe();

    }

    private Flux<LeaderInfoResponse> connectToInfoEndpoint(CloudService service) {
        return Flux.defer(()->this.rsHnd.apply("LeaderInfoService", CloudMessage.builder().withData(new LeaderInfoRequest(this.name)).withKey(service.nodeId()).build()))
                .doOnError(thr->logger.exception(thr,"leader info endpoint connection failed for nodeId {0}", service.nodeId()))
                .retryWhen(Retry
                        .onlyIf(ctx -> this.reg.isServiceExist(service.id()))
                        .exponentialBackoffWithJitter(Duration.ofSeconds(1), Duration.ofSeconds(20)))
                .onErrorResume(thr->Flux.empty());

    }
}
