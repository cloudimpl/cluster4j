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

import com.cloudimpl.cluster.network.XChannel;
import com.cloudimpl.cluster.network.XEventCallback;
import com.cloudimpl.cluster.network.XEventLoop;
import com.cloudimpl.cluster.network.XTcpClient;
import io.netty.buffer.ByteBuf;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

/**
 *
 * @author nuwansa
 */
public class XReactiveServerSocket implements XEventCallback,Closeable {

    private final XEventLoop loop;
    private final int port;
    private XChannel channel;
    private MonoSink<XReactiveServerSocket> bindEmitter;
    private final Mono<XReactiveServerSocket> mono;
    private Consumer<XReactiveTcpServerClient> acceptor;
    public XReactiveServerSocket(XEventLoop loop, int port) {
        this.loop = loop;
        this.port = port;
        this.mono = Mono.create(em -> setBindEmitter(em));
    }

    @Override
    public void onReadEvent(XEventLoop loop, XTcpClient channel, ByteBuf buf, int len) {
        XReactiveTcpServerClient rClient = channel.getAttachment();
        rClient.emitReadEvent(buf);
    }

    @Override
    public void onDisconnect(XEventLoop loop, XTcpClient channel) {
        XReactiveTcpServerClient rClient = channel.getAttachment();
        rClient.dataEmitter.error(new ClosedChannelException());
    }

    @Override
    public void onConnect(XEventLoop loop, XTcpClient channel) {
        System.out.println("connected: "+channel);
        XReactiveTcpServerClient rclient = new XReactiveTcpServerClient(channel);
        channel.setAttachment(rclient);
        this.acceptor.accept(rclient);
    }
    
    public  Mono<XReactiveServerSocket> listen()
    {
        return mono;
    }
    
    public XReactiveServerSocket doOnConnection(Consumer<XReactiveTcpServerClient>  acceptor)
    {
        this.acceptor = acceptor;
        return this;
    }
    
    @Override
    public void onWriteReady(XEventLoop loop, XTcpClient channel) {
    }

    private void setBindEmitter(MonoSink<XReactiveServerSocket> em) {
        if (this.bindEmitter != null) {
            em.error(new IllegalStateException("XReactiveTcpClient allows only a single Subscriber"));
        }
        this.channel = XChannel.createTcpServer(port, this);
        loop.add(this.channel);
        this.bindEmitter = em;
    }

    @Override
    public void close() throws IOException {
        this.channel.close();
    }
}
