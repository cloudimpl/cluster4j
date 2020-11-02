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
package com.cloudimpl.cluster.network.rsocket;

import com.cloudimpl.cluster.network.XEventLoop;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class XClientTransport implements ClientTransport{
    private final XReactiveTcpClient client;
    public XClientTransport(XEventLoop loop,String host,int port)
    {
       this.client = new XReactiveTcpClient(loop, host, port);
    }
    
    public static XClientTransport create(XEventLoop loop,String host,int port)
    {
        return new XClientTransport(loop, host, port);
    }
    
    @Override
    public Mono<DuplexConnection> connect() {
       return this.client.connection().map(c->new XDuplexConnnection(client));
    }
    
}
