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
package com.cloudimpl.cluster.network.rsocket.test;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.DefaultPayload;
import java.util.logging.Level;
import java.util.logging.Logger;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

/**
 *
 * @author nuwansa
 */
public class Test {

    public static Mono<Payload> request(RSocket socket)
    {
        return socket.requestResponse(DefaultPayload.create("nuwan".getBytes()));
    }
    public static FluxSink<Payload> sink;

    public static void setSink(FluxSink<Payload> sink) {
        Test.sink = sink;
    }
    
    
    public static void main(String[] args) {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    root.setLevel(ch.qos.logback.classic.Level.INFO);
      //  int offset = Unsafe.getUnsafe().arrayBaseOffset(byte[].class);
        System.setProperty( "io.netty.tryReflectionSetAccessible","true") ;
       // System.setProperty( "io.netty.leakDetection.level","DISABLED") ;
        Payload p = ByteBufPayload.create("hello");
        
        Flux<Payload> f = Flux.create(em->setSink(em));
        Thread t2 = new Thread(()->{
            while(true)
            {
                p.retain();
                if(Test.sink != null)
                 Test.sink.next(p);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ex) {
                    Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
           
        });
        t2.start();
        Mono<Payload> resp = Mono.just(p);
        RSocket rsocket
                = new RSocket() {
            boolean fail = true;

            @Override
            public Flux<Payload> requestStream(Payload p) {
                System.out.println("$$$$$$$$$$request received "+p.getDataUtf8());
                p.release();
                return f;
            }
        };

        TcpServer server = TcpServer.create().host("127.0.0.1").port(7000)
              //  .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                ;

        RSocketServer.create(SocketAcceptor.with(rsocket))
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                //.bind(XServerSocketTransport.create(loop, "127.0.0.1", 7000))
                 .bind(TcpServerTransport.create(server))
                .subscribe();

      //  Mono<RSocket> socket =
      Thread t = new Thread(()->{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
            }
            TcpClient tcpClient = TcpClient.create().host("127.0.0.1").port(7000)
                  //  .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    ;
          RSocketConnector.create().payloadDecoder(PayloadDecoder.ZERO_COPY)
               //   .connect(XClientTransport.create(loop, "127.0.0.1", 7000))
                  .connect(TcpClientTransport.create(tcpClient))
                .cache().flatMapMany(s->s.requestStream(ByteBufPayload.create("nuwan".getBytes()))
                     //   .doOnNext(k->System.out.println(k.getDataUtf8()))
                        .doOnNext(k->k.release())
                       
                )
                  .doOnError(thr->thr.printStackTrace()).subscribe();
      });
          t.start();

//        Thread t = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                long rate = 0;
//                long start = System.currentTimeMillis();
//                for (long i = 0; i < Long.MAX_VALUE; i++) {
//                    CountDownLatch latch = new CountDownLatch(1);
//                    socket.flatMap(s->s
//                            .requestResponse(p)
//                            .doOnNext(pl -> pl.release())
//                            .doOnNext(c->latch.countDown())
//                            .onErrorReturn(p)
//                            //   .doOnNext(System.out::println)
//                    ).subscribe();
//                    try {
//                        latch.await();
//                    } catch (InterruptedException ex) {
//                        Logger.getLogger(Test.class.getName()).log(Level.SEVERE, null, ex);
//                    }
//                    System.out.println("sending data");
//                    rate++;
//                    long end = System.currentTimeMillis();
//                    if (end - start > 1000) {
//                        System.out.println("rate :" + rate);
//                        rate = 0;
//                        start = System.currentTimeMillis();
//                    }
//                }
//            }
//        });
//        t.start();
   //     loop.run(false);
        //socket.dispose();
    }
}
