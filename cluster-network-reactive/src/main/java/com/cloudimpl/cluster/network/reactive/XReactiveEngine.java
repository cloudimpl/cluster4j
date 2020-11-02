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

import com.cloudimpl.cluster.network.AsyncTask;
import com.cloudimpl.cluster.network.XChannel;
import com.cloudimpl.cluster.network.XEventCallback;
import com.cloudimpl.cluster.network.XEventLoop;
import com.cloudimpl.cluster.network.XTcpClient;
import com.cloudimpl.cluster.network.XUdpChannel;
import io.netty.buffer.ByteBuf;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class XReactiveEngine implements XEventCallback {

    private final XEventLoop loop;
    private final Executor loopRunner;
    public XReactiveEngine(XEventLoop loop) {
        this.loop = loop;
        this.loopRunner = Executors.newSingleThreadExecutor();
    }

    public Mono<XRSocket> createClient(String ipAddr, int port) {
        AsyncTask connectSink = new AsyncTask(evtLoop -> {
        }, null,null);
        Consumer<AsyncTask> channelProvider = channelSink -> {
            XChannel channel = XChannel.createTcpClient(ipAddr, port, this);
            channel.setAttachment(connectSink);
            channelSink.getLoop().add(channel);
            channelSink.setChannel(channel);
        };
        Mono<AsyncTask> channel = this.loop.pushAsynTask(channelProvider);
        return channel.flatMap(c -> connectSink.asMono()).map(c -> c.getAttachment()).map(c->new XRSocketClient((XReactiveTcpClient)c));
    }

    public Mono<XReactiveServer> createServer(String ipAddr, int port,XRSocket socketAcceptor) {
        Consumer<AsyncTask> channelProvider = channelSink -> {
            XChannel channel = XChannel.createTcpServer(port, this);
            XReactiveServer server = new XReactiveServer(channel,socketAcceptor);
            channel.setAttachment(server);
            channelSink.getLoop().add(channel);
            channelSink.setChannel(channel);
        };
        Mono<AsyncTask> channel = this.loop.pushAsynTask(channelProvider);
        return channel.map(c -> c.getChannel().getAttachment());
    }

    public void run() {
        this.loop.run(false);
    }

    @Override
    public void onReadEvent(XEventLoop loop, XTcpClient channel, ByteBuf buf, int len) {
    //    System.out.println("ondata received :"+len);
        XReactiveTcpClient client = channel.getAttachment();
        if (client != null) {
            client.onData(buf);
        }
    }

    @Override
    public void onReadEvent(XEventLoop loop, XUdpChannel channel, ByteBuf buf, int len) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void onDisconnect(XEventLoop loop, XTcpClient channel) {
        XReactiveTcpClient client = channel.getAttachment();
        if (client != null) {
            client.onError(new ClosedChannelException());
        }
    }

    @Override
    public void onConnect(XEventLoop loop, XTcpClient channel) {
        Object attachment = channel.getAttachment();
        if (attachment != null && attachment instanceof AsyncTask) {
            AsyncTask sink = (AsyncTask) attachment;
            XReactiveTcpClient client = new XReactiveTcpClient(channel);
            sink.setAttachment(client);
            channel.setAttachment(client);
            sink.success();
        } else if (channel.getServerSocket() != null) {
            XReactiveServer server = channel.getServerSocket().getAttachment();
            XReactiveTcpClient client = new XReactiveTcpClient(channel);
            channel.setAttachment(client);
            server.emit(client);
        }
    }

    @Override
    public void onWriteReady(XEventLoop loop, XTcpClient channel) {
        //  throw new UnsupportedOperationException("Not supported yet.");
    }

}
