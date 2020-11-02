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
package com.cloudimpl.cluster.seed;

import com.cloudimpl.cluster.common.FluxStream;
import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.common.RouterType;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import com.cloudimpl.cluster4j.core.annon.Router;
import com.cloudimpl.cluster4j.coreImpl.CloudServiceRegistry;
import com.cloudimpl.cluster4j.logger.Logger;
import java.util.function.Function;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
@CloudFunction(name = "SeedService")
@Router(routerType = RouterType.ROUND_ROBIN)
public class SeedService implements Function<CloudMessage, Mono<String>>{

    private final CloudServiceRegistry serviceRegistry;

    @Inject
    public SeedService(CloudServiceRegistry registry,Logger logger) {
        this.serviceRegistry = registry;
         serviceRegistry.flux().filter(e -> e.getType() == FluxStream.Event.Type.ADD)
        .map(e -> e.getValue())
        .doOnNext(srv -> logger.info("service {0} with id {1} added", srv.name(),srv.id()))
        .subscribe();
    serviceRegistry.flux().filter(e -> e.getType() == FluxStream.Event.Type.REMOVE)
        .map(e -> e.getValue())
        .doOnNext(srv -> logger.info("service {0} with id {1} removed", srv.name(),srv.id()))
        .subscribe();
    }

    @Override
    public Mono<String> apply(CloudMessage t) {
        return Mono.empty();
    }
    
}
