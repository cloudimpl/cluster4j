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
package com.cloudimpl.cluster4j.routers;

import com.cloudimpl.cluster.common.FluxStream;
import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.core.CloudRouter;
import com.cloudimpl.cluster4j.core.CloudService;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.Named;
import com.cloudimpl.cluster4j.core.RouterException;
import com.cloudimpl.cluster4j.coreImpl.CloudServiceRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class NodeIdRouter implements CloudRouter {

    private final Map<String, CloudService> nodeIdTopics = new ConcurrentHashMap<>();
    private final String topic;

    @Inject
    public NodeIdRouter(@Named("@topic") String topic, CloudServiceRegistry serviceRegistry) {
        this.topic = topic;
        serviceRegistry.flux().filter(e -> e.getType() == FluxStream.Event.Type.ADD || e.getType() == FluxStream.Event.Type.UPDATE)
                .map(e -> e.getValue()).filter(e -> e.name().equals(topic)).doOnNext(e -> nodeIdTopics.put(e.nodeId(), e)).subscribe();
        serviceRegistry.flux().filter(e -> e.getType() == FluxStream.Event.Type.REMOVE)
                .map(e -> e.getValue()).filter(e -> e.name().equals(topic)).doOnNext(e -> nodeIdTopics.remove(e.nodeId())).subscribe();
    }

    @Override
    public Mono<CloudService> route(CloudMessage msg) {
        CloudService srv = nodeIdTopics.get(msg.getKey());
        if (srv != null) {
            return Mono.just(srv);
        } else {
            return Mono.error(new RouterException("service not found to route for topic [" + topic + "]"));
        }
    }

}
