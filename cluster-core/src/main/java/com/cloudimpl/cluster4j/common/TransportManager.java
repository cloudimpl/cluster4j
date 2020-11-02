/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

import com.cloudimpl.cluster.common.FluxMap;
import com.cloudimpl.cluster.common.FluxStream.Event;
import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class TransportManager {
  private final FluxMap<RouteEndpoint, Mono<RSocket>> mapConnections = new FluxMap<>();
  private final MessageCodec defaultCodec;

  public TransportManager(MessageCodec defaultCodec) {
    this.defaultCodec = defaultCodec;
  }


  public Mono<RSocket> get(RouteEndpoint endpoint) {
    return get(endpoint, defaultCodec);
  }

  public Mono<RSocket> get(RouteEndpoint endpoint, MessageCodec codec) {
    Mono<RSocket> socket = mapConnections.computeIfAbsent(endpoint, this::connect);
    return socket;
  }

  public void createEndpoint(String host, int port, EndpointListener listener) {
    createEndpoint(host, port, defaultCodec, listener);
  }

  public Flux<Event<RouteEndpoint,Mono<RSocket>>> connectionsFlux()
  {
      return mapConnections.flux();
  }
  
  public void createEndpoint(String host, int port, MessageCodec codec, EndpointListener listener) {
    RSocketFactory.receive()
        // .frameDecoder(PayloadDecoder.ZERO_COPY)
        .acceptor((SocketAcceptor) new SocketAcceptorImpl(codec, listener))
         
        .transport(TcpServerTransport.create(host, port)).start()
            .doOnNext(c->listener.onInit(c.address()))
        .subscribe();
  }

  private Mono<RSocket> connect(RouteEndpoint endpoint) {
    return connectRemote(endpoint);
  }

  private Mono<RSocket> handleErrors(Mono<RSocket> mono, RouteEndpoint endpoint) {
    return mono.doOnSuccess(
        rsocket -> {
          System.out.println("Connected successfully on " + endpoint);
          // setup shutdown hook
          rsocket
              .onClose()
              .doOnTerminate(
                  () -> {
                    mapConnections.remove(endpoint);
                    System.out.println("Connection closed on {} and removed from the pool " + endpoint);
                  })
              .subscribe();
        })
        .doOnError(
            throwable -> {
              System.out.println("Connect failed on {}, cause: " + endpoint + " " + throwable);
              mapConnections.remove(endpoint);
            })
        .cache();
  }

  private Mono<RSocket> connectRemote(RouteEndpoint endpoint) {
    Mono<RSocket> rsocketMono =
        RSocketFactory.connect()
            // .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(TcpClientTransport.create(endpoint.getHost(), endpoint.getPort())).start();
    return handleErrors(rsocketMono, endpoint);
  }

  private static class SocketAcceptorImpl<T> implements SocketAcceptor {

    private final MessageCodec codec;
    private final EndpointListener<T> listener;

    public SocketAcceptorImpl(MessageCodec codec, EndpointListener<T> listener) {
      this.codec = codec;
      this.listener = listener;
    }

    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
      System.out.println("socket connected..."+setupPayload.getDataUtf8());
      
      return Mono.just(new AbstractRSocket() {

        @Override
        public Mono<Void> fireAndForget(Payload payload) {
          return listener.fireAndForget((T) decode(payload));
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
          return listener.requestResponse((T) decode(payload)).map(this::encode);
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
          return listener.requestStream((T) decode(payload)).map(this::encode);
        }

        @Override
        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
          return listener.requestChannel((Publisher<T>) Flux.from(payloads).map(s->this.decode(s))).map(this::encode);
        }

        private Payload encode(Object msg) {
          return DefaultPayload.create(codec.encode(msg));
        }

        private Object decode(Payload payload) {
          return codec.decode(payload);
          // payload.release();
        }
      });
    }
  }
}
