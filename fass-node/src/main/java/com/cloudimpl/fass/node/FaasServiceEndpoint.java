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
import com.cloudimpl.cluster4j.common.EndpointListener;
import com.cloudimpl.cluster4j.core.CloudService;
import com.cloudimpl.cluster4j.core.CloudUtil;
import com.cloudimpl.cluster4j.core.Inject;
import com.cloudimpl.cluster4j.coreImpl.CloudEngine;
import com.cloudimpl.cluster4j.coreImpl.CloudServiceRegistry;
import com.cloudimpl.cluster4j.coreImpl.ServiceEndpointPlugin;
import com.cloudimpl.fn.core.impl.FaasServiceHeaders;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class FaasServiceEndpoint implements ServiceEndpointPlugin{
    
    private final CloudServiceRegistry registry;
    private CloudService faasClientService;
    @Inject
    public FaasServiceEndpoint(CloudServiceRegistry registry) {
        this.registry = registry;
    }
    
    @Override
    public int getServicePort() {
        return 11000;
    }

    @Override
    public String getHostAddr() {
        return CloudUtil.getHostIpAddr();
    }

    private CloudService getService()
    {
        if(faasClientService == null)
            faasClientService = registry.findLocalByName(FaasServiceHeaders.FAAS_SERVICE_NAME);
        return faasClientService;
    }
    @Override
    public EndpointListener getEndpointListener(CloudEngine engine) {

        return new EndpointListener<CloudMessage>() {
            @Override
            public Mono<Void> fireAndForget(CloudMessage msg) {
                return getService().send(msg);
            }

            @Override
            public Mono<CloudMessage> requestResponse(CloudMessage msg) {
                return getService().requestReply(msg)
                        .map(d -> CloudMessage.builder().withData(d).build());
            }

            @Override
            public Flux<CloudMessage> requestStream(CloudMessage msg) {
                return getService().requestStream(msg)
                        .map(d -> CloudMessage.builder().withData(d).build());
            }

        };
    }

    @Override
    public String name() {
        return "FAAS-ENDPOINT";
    }
    
}
