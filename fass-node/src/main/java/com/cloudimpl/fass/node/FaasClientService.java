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
package com.cloudimpl.fass.node;

import com.cloudimpl.cluster4j.common.CloudMessage;
import com.cloudimpl.cluster4j.common.JsonMessageCodec;
import com.cloudimpl.cluster4j.common.RouteEndpoint;
import com.cloudimpl.cluster4j.common.RouterType;
import com.cloudimpl.cluster4j.common.TransportManager;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.core.annon.CloudFunction;
import com.cloudimpl.cluster4j.core.annon.Router;
import com.cloudimpl.fn.core.impl.FaasServiceHeaders;
import com.cloudimpl.fn.core.msgs.PodDetails;
import com.cloudimpl.fn.core.msgs.PodLogin;
import io.rsocket.util.DefaultPayload;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
@CloudFunction(name = FaasServiceHeaders.FAAS_SERVICE_NAME)
@Router(routerType = RouterType.NODE_ID)
public class FaasClientService implements Function<CloudMessage,Publisher<CloudMessage>>{

    private final TransportManager transportManager;
    private  RouteEndpoint endpoint;
    @Inject
    public FaasClientService(TransportManager transportManager) {
        this.transportManager = transportManager;
    }

    
    @Override
    public Publisher<CloudMessage> apply(CloudMessage t) {
        System.out.println("faas function received message ."+t.data().getClass().getName()+""+t.data());
        return init(t.data());
      //  return transportManager.get(endpoint)
    }
    
    
    private Mono<CloudMessage> init(PodDetails podDetails)
    {
        RouteEndpoint e = RouteEndpoint.create(podDetails.getPodIp(), FaasServiceHeaders.FAAS_SERVER_PORT);
        return transportManager.get(e)
                .flatMap(s->s.requestResponse(DefaultPayload.create(JsonMessageCodec.instance().encode(new PodLogin("xx", "xx","xxx")))))
                .doOnNext(r->setEndpoint(e)).map(s->CloudMessage.builder().withData(s).build());
    }
    
    private void setEndpoint(RouteEndpoint endpoint)
    {
        this.endpoint = endpoint;
    }
}
