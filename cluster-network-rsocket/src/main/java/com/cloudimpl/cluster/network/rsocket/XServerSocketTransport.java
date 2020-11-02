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
import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import java.util.Objects;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class XServerSocketTransport implements ServerTransport<Closeable>{
    private final XReactiveServerSocket server;
    public XServerSocketTransport(XReactiveServerSocket server) {
        this.server = server;
    }
    
    public static XServerSocketTransport create(XEventLoop loop,String host,int port)
    {
        return new XServerSocketTransport(new XReactiveServerSocket(loop, port));
    }
    
    @Override
    public Mono<Closeable> start(ConnectionAcceptor acceptor) {
        Objects.requireNonNull(acceptor, "acceptor must not be null");
   
     return server.doOnConnection(c->{
         acceptor.apply(new XDuplexConnnection(c)).then(Mono.<Void>never())
                  .subscribe((VOID)->c.close());
     })
    .listen().cast(Closeable.class);
    }

}
