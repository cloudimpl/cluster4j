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

import com.cloudimpl.cluster.network.XEventCallback;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.rsocket.internal.BaseDuplexConnection;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class XDuplexConnnection extends BaseDuplexConnection implements XEventCallback {

    private final XReactiveTcpClientBase client;

    public XDuplexConnnection(XReactiveTcpClientBase client) {
        this.client = client;
    }
    
    @Override
    protected void doOnClose() {
        try {
            this.client.close();
        } catch (IOException ex) {
            Logger.getLogger(XDuplexConnnection.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public Mono<Void> send(Publisher<ByteBuf> frames) {
        return this.client.send(alloc(),frames);
    }

    @Override
    public Flux<ByteBuf> receive() {
        return this.client.dataFlux().map(frame->frame.skipBytes(3));
                //.doOnNext(f->f.retain());
                //.doOnNext(f->System.out.println("frame receiving : "+f.readableBytes()));
    }

    @Override
    public ByteBufAllocator alloc() {
        return PooledByteBufAllocator.DEFAULT;
    }
            
}
