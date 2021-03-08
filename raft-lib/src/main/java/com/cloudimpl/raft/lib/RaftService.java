/*
 * Copyright 2021 nuwan.
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
package com.cloudimpl.raft.lib;

import com.cloudimpl.raft.lib.msg.RaftMsg;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwan
 */
public class RaftService implements Function<RaftMsg,Mono<RaftMsg>>{
    private Disposable electionTimer;
    private int electionInterval;

    public RaftService() {
        electionInterval = 5000;
        initElectionTimer();
    }
    
    
    @Override
    public Mono<RaftMsg> apply(RaftMsg msg) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    
    private void initElectionTimer()
    {
        electionTimer = Flux.interval(Duration.ofMillis(electionInterval + getRnd()))
                .doOnNext(i->onElectionTimer())
                .subscribe();
    }
    
    
    private void onElectionTimer()
    {
        
    }
    
    private int getRnd()
    {
        return ThreadLocalRandom.current().nextInt(100, 1000);
    }
}
