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
import java.nio.channels.ClosedChannelException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

/**
 *
 * @author nuwansa
 */
public class XReactiveTcpClient extends XReactiveTcpClientBase implements XEventCallback{

    private MonoSink<XReactiveTcpClient> connectionEmitter;
    private final Mono<XReactiveTcpClient> mono;
    private final XEventLoop loop;
    private final String hostAddr;
    private final int port;
    public XReactiveTcpClient(XEventLoop loop, String hostAddr, int port) {
        super(null);
        this.loop = loop;
        this.hostAddr = hostAddr;
        this.port = port;
        this.mono = Mono.create(em -> setConnEmitter(em));
    }

    public Mono<XReactiveTcpClient> connection() {
        return mono;
    }

    @Override
    public void onConnect(XEventLoop loop, XTcpClient channel) {
        System.out.println("socket connected");
        this.connectionEmitter.success(this);
    }

    private void setConnEmitter(MonoSink<XReactiveTcpClient> connEmitter) {
        if (this.connectionEmitter != null) {
            connEmitter.error(new IllegalStateException("XReactiveTcpClient allows only a single Subscriber"));
        }
        this.client = XChannel.createTcpClient(hostAddr, port, this);
        loop.pushAsynTask(task->{
            task.getLoop().add(client);
        }).subscribe();
        this.connectionEmitter = connEmitter;

    }

    @Override
    public void onReadEvent(XEventLoop loop, XTcpClient channel, ByteBuf buf, int len) {
        emitReadEvent(buf);
    }

    @Override
    public void onDisconnect(XEventLoop loop, XTcpClient channel) {
        dataEmitter.error(new ClosedChannelException());
    }

    @Override
    public void onWriteReady(XEventLoop loop, XTcpClient channel) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() {
        this.client.close();
    }
}
