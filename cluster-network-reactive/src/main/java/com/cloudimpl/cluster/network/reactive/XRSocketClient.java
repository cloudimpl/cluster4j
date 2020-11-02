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

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class XRSocketClient implements XRSocket {

    private final XReactiveTcpClient client;

    protected XRSocketClient(XReactiveTcpClient client) {
        this.client = client;
    }

    @Override
    public Mono<ByteBuf> requestReply(ByteBuf buf) {
    //    System.out.println("request reply called");
        long streamId = client.getStreamIdProvider().getNextId();
        ByteBuf sendFrame = client.getFrameHandler().encodeFrame(streamId, true, 1, buf);
        return this.client.asKeyFlux()
//                .doFirst(() ->{
//            writeToClient(sendFrame);
//            System.out.println("writing to stream :"+streamId);
//            sendFrame.release();
//        })
                .contextWrite(ctx->ctx.put("key", streamId).put("supplier",(Runnable)()->{writeToClient(sendFrame);sendFrame.release();}))
               // .doOnNext(frame->System.out.println("receiving resp:"+client.getFrameHandler().getStreamId(frame)))
          //      .groupBy(frame->this.client.getFrameHandler().getStreamId(frame))
                .filter(frame->frame.refCnt() != 0)
                .filter(frame -> !client.getFrameHandler().isRequest(frame))
                .filter(frame -> filterStream(streamId, frame))
                .next()
                .doOnError(thr->thr.printStackTrace());
                //.doOnTerminate(()->System.out.println("terminated:"+streamId));
    }

    private boolean filterStream(long streamId, ByteBuf frame) {
        boolean b =  streamId == this.client.getFrameHandler().getStreamId(frame);
      //  System.out.println("stream Id : "+streamId + " ok : "+b);
        return b;
    }

    
    private void writeToClient(ByteBuf frame)
    {
     //    System.out.println("write client :");
        this.client.getChannel().write(frame);
    }
}
