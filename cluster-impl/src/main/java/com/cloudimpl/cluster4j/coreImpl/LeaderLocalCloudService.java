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
import com.cloudimpl.cluster4j.core.CloudServiceDescriptor;
import com.cloudimpl.cluster4j.core.CloudUtil;
import com.cloudimpl.cluster4j.core.Injector;
import com.cloudimpl.cluster4j.core.LeaderLifeCycle;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import com.cloudimpl.cluster4j.le.LeaderElection;
import com.cloudimpl.cluster4j.le.LeaderElectionManager;
import java.util.function.Supplier;

/**
 *
 * @author nuwansa
 */
public class LeaderLocalCloudService extends LocalCloudService {

    private final LeaderElectionManager elMan;
    private final ILogger logger;
    private LeaderElection el;
    private final CloudServiceRegistry reg;
    public LeaderLocalCloudService(Supplier<String> memberIdProvider, String nodeId, Injector injector,
             CloudServiceDescriptor descriptor, LeaderElectionManager elMan, ILogger logger,CloudServiceRegistry reg) {
        super(memberIdProvider, nodeId, injector, descriptor);
        this.reg = reg;
        this.elMan = elMan;
        this.logger = logger;
    }

    @Override
    public void init() {
        this.el = this.elMan.create(this.name, this.id, 10000, logger);
        Injector inject = injector.with("@srvId", id).with("@srvName", name)
                .with(LeaderElection.class, el);
        this.function = CloudUtil.newInstance(inject, descriptor.getFunctionType());
        if(this.function instanceof LeaderLifeCycle)
        {
            LeaderLifeCycle leaderListner = (LeaderLifeCycle)this.function;
            el.run((LeaderElection e , String leaderId)->leaderListner.onLeaderElect(leaderId));
        }
        else
        {
            el.run((LeaderElection e , String leaderId)->{});
        }
        watchOthers();
    }
    
    private void watchOthers()
    {
        reg.flux().filter(e->e.getType() == FluxStream.Event.Type.ADD | e.getType() == FluxStream.Event.Type.UPDATE)
                .map(e->e.getValue())
                .filter(s->s.name().equals(this.name))
                .filter(s->s != this).doOnNext(s->this.el.addMember(s.id())).subscribe();
        reg.flux().filter(e->e.getType() == FluxStream.Event.Type.REMOVE)
                .map(e->e.getValue())
                .filter(s->s.name().equals(this.name))
                .filter(s->s != this).doOnNext(s->this.el.removeMember(s.id())).subscribe();
    }
}
