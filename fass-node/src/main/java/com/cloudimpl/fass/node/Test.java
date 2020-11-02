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

import com.cloudimpl.cluster4j.common.MessageCodec;
import com.cloudimpl.cluster4j.common.Msg;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import java.nio.ByteBuffer;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class Test {

    public static class SimpleMessageCodec implements MessageCodec {

        @Override
        public Object decode(Payload payload) {
            return payload.getDataUtf8();
        }

//  @Override
//  public <T> T decode(Class<T> cls, ByteBuffer buffer) {
//    byte[] bytesArray = new byte[buffer.remaining()];
//    buffer.get(bytesArray, 0, bytesArray.length);
//    return GsonCodec.decode(cls, new String(bytesArray));
//  }
        @Override
        public ByteBuffer encode(Object obj) {
            return ByteBuffer.wrap(((String)obj).getBytes());
        }

    }

    public static void main(String[] args) throws InterruptedException {
//        DirectProcessor<String> processotr = DirectProcessor.create();
//        processotr
//                .doOnError(thr->thr.printStackTrace())
//               // .doOnNext(System.out::println)
//                .subscribe();
//        FluxSink<String> sink = processotr.sink();
//        int i = 0;
//       while(true)
//       {
//           String data = "nuwan"+i;
//           sink.next(data);
//           //Thread.sleep(10);
//       }


//
//        SimpleMessageCodec codec = new SimpleMessageCodec();
//        TransportManager transport = new TransportManager(codec);
//        transport.createEndpoint("0.0.0.0", 1234, new EndpointListener<String>() {
//            @Override
//            public Mono<String> requestResponse(String msg) {
//                return Mono.just(msg);
//            }
//        });
//
//        //Mono<RSocket> socket = transport.get(RouteEndpoint.create("0.0.0.0", 1234));
//        Payload payload = DefaultPayload.create("nuwan");
//        while (true) {
//            transport.get(RouteEndpoint.create("0.0.0.0", 1234)).flatMap(socket -> socket.requestResponse(payload))
//                    //.map(p->codec.decode(p))
//                    .doOnError(thr -> thr.printStackTrace())
//                  //  .doOnNext(System.out::println)
//                    .subscribe();
//           // sleep(1000);
//        }
       
      //  String s = "nuwan";
      Payload p = DefaultPayload.create(new String(new byte[1024]));
      Mono<Payload> resp = Mono.just(p);
      RSocket rsocket =
        new RSocket() {
          boolean fail = true;

          @Override
          public Mono<Payload> requestResponse(Payload p) {
              p.release();
            return resp;
          }
        };

    RSocketServer.create(SocketAcceptor.with(rsocket))
            .payloadDecoder(PayloadDecoder.ZERO_COPY)
        .bind(TcpServerTransport.create("localhost", 7000))
        .subscribe();

    RSocket socket =
        RSocketConnector.create().payloadDecoder(PayloadDecoder.ZERO_COPY).connect(TcpClientTransport.create("localhost", 7000)).block();

    long rate = 0;
    long start = System.currentTimeMillis();
    for (long i = 0; i < Long.MAX_VALUE ; i++) {
      socket
          .requestResponse(p)
          .doOnNext(pl->pl.release())    
          .onErrorReturn(p)
       //   .doOnNext(System.out::println)
          .block();
      rate++;
      long end = System.currentTimeMillis();
      if(end -start > 1000)
      {
          System.out.println("rate :"+rate);
          rate = 0;
          start = System.currentTimeMillis();
      }
    }

    socket.dispose();
        
    }
}

class Student implements Msg {

    private String a = "nuwa";
    private Object m = new Address();
}

class Address implements Msg {

    String b = "asfaf";
}
