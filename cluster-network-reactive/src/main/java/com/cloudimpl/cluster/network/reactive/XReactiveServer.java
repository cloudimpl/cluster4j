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
package com.cloudimpl.cluster.network.reactive;

import com.cloudimpl.cluster.network.XChannel;
import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public class XReactiveServer{
    private final XChannel channel;
    private final FluxProcessor<XReactiveTcpClient> clientProcessor;
    private final XRSocket socketAcceptor;
    public XReactiveServer(XChannel channel,XRSocket socketAcceptor) {
        this.channel = channel;
        this.clientProcessor = new FluxProcessor<>();
        this.socketAcceptor = socketAcceptor;
    }
    
    public Flux<XReactiveTcpClient> getReactiveSocketAcceptor()
    {
        return this.clientProcessor.asFlux();
    }

    public XRSocket getSocketAcceptor()
    {
        return socketAcceptor;
    }
    
    protected void emit(XReactiveTcpClient client)
    {
        this.clientProcessor.emit(client);
    }
}
