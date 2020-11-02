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

import com.cloudimpl.cluster.network.XTcpClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.rsocket.frame.FrameLengthCodec;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;
import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_SIZE;
import java.io.Closeable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public abstract class XReactiveTcpClientBase implements Closeable {

    protected FluxSink<ByteBuf> dataEmitter;
    private final Flux<ByteBuf> flux;
    protected XTcpClient client;
    private ByteBuf cacheBuf;

    public XReactiveTcpClientBase(XTcpClient client) {
        this.client = client;
        this.flux = Flux.create(em -> {
            setDataEmitter(em);
        });
    }

    private void setDataEmitter(FluxSink<ByteBuf> em) {
        if (this.dataEmitter != null) {
            em.error(new IllegalStateException("XReactiveTcpClient allows only a single Subscriber"));
        }
        this.dataEmitter = em;
    }

    public Flux<ByteBuf> dataFlux() {
        return flux;
    }

    protected void emitReadEvent(ByteBuf buf)
    {
        if(cacheBuf != null)
            cacheBuf.writeBytes(buf);
        else
            cacheBuf = buf;
        int frameSize = decodeFrameSize(cacheBuf);
      //  System.out.println("frame len :"+frameSize + " received, buf len : "+cacheBuf.readableBytes());
        while(cacheBuf.readableBytes() >= frameSize)
        {
            ByteBuf frameBuf = PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 4, 1024 * 1024);
            frameBuf.writeBytes(cacheBuf,0,frameSize);
            cacheBuf.readerIndex(frameSize);
            dataEmitter.next(frameBuf);
            cacheBuf.discardReadBytes();
            frameSize = decodeFrameSize(cacheBuf);
       //     System.out.println("frame len :"+frameSize + " received, buf len : "+cacheBuf.readableBytes());
        }
        if(cacheBuf == buf && cacheBuf.readableBytes() > 0)
        {
            ByteBuf newCacheBuf = PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 4, 1024 * 1024);
            newCacheBuf.writeBytes(cacheBuf,cacheBuf.readableBytes());
            cacheBuf = newCacheBuf;
        }
        else if(cacheBuf.readableBytes() == 0 && cacheBuf != buf)
        {
            cacheBuf.release();
            cacheBuf = null;
        }
    }
    
    private int decodeFrameSize(ByteBuf buf)
    {
        if(buf.readableBytes() < 3)
            return FRAME_LENGTH_SIZE;
        return FrameLengthCodec.length(cacheBuf) + FRAME_LENGTH_SIZE;
    }
    
    public Mono<Void> send(ByteBufAllocator alloc, Publisher<ByteBuf> frames) {
        if (frames instanceof Mono) {
            return Mono.from(frames).map(frame -> this.copyToNative(alloc, frame)).doOnNext(b -> client.write(b)).doOnNext(b -> b.release()).then();
        } else {
            return Flux.from(frames).map(frame -> this.copyToNative(alloc, frame)).doOnNext(b -> client.write(b)).doOnNext(b -> b.release()).then();
        }
    }

    private ByteBuf copyToNative(ByteBufAllocator alloc, ByteBuf frame) {
        ByteBuf encodeBuf = alloc.directBuffer(1024 * 4, 1024 * 1024);
      //  System.out.println("client encoding length: " + frame.readableBytes() + " client : "+this.getClass().getName());
        encodeLength(encodeBuf, frame.readableBytes());
        encodeBuf.writeBytes(frame);
        frame.release();
        return encodeBuf;
    }

    private static void encodeLength(final ByteBuf byteBuf, final int length) {
        if ((length & ~FRAME_LENGTH_MASK) != 0) {
            throw new IllegalArgumentException("Length is larger than 24 bits");
        }
        // Write each byte separately in reverse order, this mean we can write 1 << 23 without
        // overflowing.
        byteBuf.writeByte(length >> 16);
        byteBuf.writeByte(length >> 8);
        byteBuf.writeByte(length);
    }
}
