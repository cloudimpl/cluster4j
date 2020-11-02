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
package com.cloudimpl.fn.core.impl;

import com.cloudimpl.cluster.common.FluxStream;
import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.common.EndpointListener;
import com.cloudimpl.cluster4j.common.GsonCodec;
import com.cloudimpl.cluster4j.common.JsonMessageCodec;
import com.cloudimpl.cluster4j.common.RouteEndpoint;
import com.cloudimpl.cluster4j.common.TransportManager;
import com.cloudimpl.cluster4j.core.CloudUtil;
import com.cloudimpl.fn.core.Handler;
import com.cloudimpl.fn.core.msgs.FaasMeta;
import io.rsocket.util.DefaultPayload;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class CloudRuntimeImpl implements com.cloudimpl.fn.core.CloudRuntime, EndpointListener<CloudMessage> {

    private final TransportManager transportManager = new TransportManager(new JsonMessageCodec());
    private InetSocketAddress serverAddress;
    private final Map<String, Class<? extends Handler>> mapHandlers = new HashMap<>();
    private RouteEndpoint faasRoute = null;

    public CloudRuntimeImpl() {
        listenToConnections();
    }

    @Override
    public void register(String handlerName, Class<? extends Handler> handlerType) {
        Class<? extends Handler> old = mapHandlers.putIfAbsent(handlerName, handlerType);
        if (old != null) {
            throw new CloudFunctionException("handler already exist .name: " + handlerName);
        }
    }

    @Override
    public void start() {
        transportManager.createEndpoint(CloudUtil.getHostIpAddr(), FaasServiceHeaders.FAAS_SERVER_PORT, this);
        while(true)
        {
            try {
                Thread.sleep(100000);
            } catch (InterruptedException ex) {
                
            }
        }
    }

    @Override
    public void onInit(InetSocketAddress addr) {
        this.serverAddress = addr;
    }

    @Override
    public Mono<Void> fireAndForget(CloudMessage msg) {
        return Mono.error(new UnsupportedOperationException("Fire and forget not implemented."));
    }

    @Override
    public Mono<CloudMessage> requestResponse(CloudMessage msg) {
        return Mono.just(CloudMessage.builder().withData("received").build());
       // return msg.doOnNext(m->System.out.println("msg received :"+m.data())).map(m->CloudMessage.builder().withData("received").build());
    }

    @Override
    public Flux<CloudMessage> requestStream(CloudMessage msg) {
        return Flux.error(new UnsupportedOperationException("Request-Stream not implemented."));
    }

    private void listenToConnections() {
        transportManager.connectionsFlux().filter(e -> e.getType() == FluxStream.Event.Type.ADD || e.getType() == FluxStream.Event.Type.UPDATE)
                .map(e -> e.getKey()).doOnNext(e -> cacheRoute(e)).subscribe();
        transportManager.connectionsFlux().filter(e -> e.getType() == FluxStream.Event.Type.REMOVE)
                .map(e -> e.getKey()).doOnNext(e -> cacheRoute(null)).subscribe();
    }

    private void cacheRoute(RouteEndpoint route) {
        this.faasRoute = route;
    }

    private RouteEndpoint getRoute() {
        if (this.faasRoute == null) {
            FaasMeta meta = FileUtil.loadFaasMeta();
            this.faasRoute = RouteEndpoint.create(meta.getPodIp(), meta.getPodPort());
        }
        return this.faasRoute;
    }

    private Mono<CloudMessage> _request(CloudMessage msg) {
        return transportManager.get(getRoute())
                .flatMap(r -> r.requestResponse(DefaultPayload.create(GsonCodec.encode(msg)))
                .map(p -> GsonCodec.decode(CloudMessage.class, p.getDataUtf8())));
    }

    private Flux<CloudMessage> _requestStream(CloudMessage msg) {
        return transportManager.get(getRoute())
                .flatMapMany(r -> r.requestStream(DefaultPayload.create(GsonCodec.encode(msg)))
                .map(p -> GsonCodec.decode(CloudMessage.class, p.getDataUtf8())));
    }

    private Mono<Void> _fireAndForget(CloudMessage msg) {
        return transportManager.get(getRoute())
                .flatMap(r -> r.fireAndForget(DefaultPayload.create(GsonCodec.encode(msg))));
    }
}
