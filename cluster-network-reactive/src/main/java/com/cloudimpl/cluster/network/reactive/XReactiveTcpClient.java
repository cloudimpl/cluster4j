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

import com.cloudimpl.cluster.network.XTcpClient;
import io.netty.buffer.ByteBuf;
import io.rsocket.Closeable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class XReactiveTcpClient implements Closeable{
    private final  XTcpClient channel;
    private final FluxKeyProcessor fluxSink ;
    private final FrameHandler frameHandler;
    private final StreamIdProvider streamIdProvider;
    public XReactiveTcpClient(XTcpClient channel) {
        this.channel = channel;
        this.fluxSink = new FluxKeyProcessor();
        this.frameHandler = new FrameHandler();
        this.streamIdProvider = new StreamIdProvider();
        listenToServerSide();
    }
   
    protected void onData(ByteBuf buf)
    {
       this.frameHandler.onData(channel.getServerSocket() == null,buf, fluxSink);
    }
   
    public Flux<ByteBuf> asFlux()
    {
        return fluxSink.asFlux();
    }
    
    public Flux<ByteBuf> asKeyFlux()
    {
        return fluxSink.asKeyFlux();
    }
    
    
    protected void onError(Throwable thr)
    {
        this.fluxSink.emit(thr);
    }
    
    @Override
    public Mono<Void> onClose() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void dispose() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    protected StreamIdProvider getStreamIdProvider()
    {
        return this.streamIdProvider;
    }
    
    protected FrameHandler getFrameHandler()
    {
        return frameHandler;
    }

    protected XTcpClient getChannel() {
        return channel;
    }
    
    private void listenToServerSide()
    {
        if(channel.getServerSocket() != null)
        {
            XReactiveServer server = channel.getServerSocket().getAttachment();
            asFlux().flatMap(frame->onFrame(server, frame)).doOnError(thr->thr.printStackTrace()).subscribe();
        }
        
    }
    
    
    private Publisher<ByteBuf> onFrame(XReactiveServer server,ByteBuf frame)
    {
        long streamId = frameHandler.getStreamId(frame);
    //   System.out.println("received req : "+streamId +" " + frame.refCnt());
        return server.getSocketAcceptor().requestReply(frame)
              //  .doOnNext(fr->System.out.println("buf "+fr.memoryAddress()+" return:"))
                .map(payload->frameHandler.encodeFrame(streamId, false, 1, payload))
                .doOnNext(fr->{
                    if(frame.refCnt() != 0)
                    {
                        throw new RuntimeException("error frame leaked");
                    }
                })
                .doOnError(err->err.printStackTrace())
                .doOnNext(fr->channel.write(fr)).doOnNext(fr->fr.release());
    }
}
