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
package com.cloudimpl.cluster.network.reactive.test;

import com.cloudimpl.cluster.network.XEventLoop;
import com.cloudimpl.cluster.network.reactive.ByteBufHandler;
import com.cloudimpl.cluster.network.reactive.XRSocket;
import com.cloudimpl.cluster.network.reactive.XReactiveEngine;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class Test {
    public static void main(String[] args) {
     //   System.setProperty( "io.netty.leakDetection.level","DISABLED") ;
        XReactiveEngine engine = new XReactiveEngine(XEventLoop.create(64));
        XRSocket socket = new XRSocket() {
            int i = 0;
            @Override
            public Mono<ByteBuf> requestReply(ByteBuf req) {
      //          System.out.println("buf "+req.memoryAddress()+" recycled:");
                boolean ok = req.release();
                i++;
                if(i % 10 == 0)
                    return Mono.error(new RuntimeException("xxx:"+i));
        //         System.out.println("ok :"+ok);
        else
                return Mono.just(ByteBufHandler.allocate().writeBytes(("hello from server"+i++).getBytes()));
            }
        };
        engine.createServer("127.0.0.1", 12345,socket)
                .flatMapMany(s->s.getReactiveSocketAcceptor()).doOnNext(s->System.out.println("connected")).subscribe();
        ByteBuf buf = ByteBufHandler.allocate();
        buf.writeBytes("nuwan".getBytes());
        engine.createClient("127.0.0.1", 12345).doOnNext(c->System.out.println("client connected"))
                .flatMapMany(s->Flux.interval(Duration.ofMillis(1000)).onBackpressureDrop().flatMap(i->s.requestReply(buf.retain())))
                .doOnNext(b->{
                    b.skipBytes(10);
                    byte[] bytes = new byte[b.readableBytes()];
                    b.readBytes(bytes);
                    System.out.println("response: "+new String(bytes));
                })
                .doOnError(thr->thr.printStackTrace())
                .doOnNext(b->b.release())
                .subscribe();
        engine.run();
    }
}
