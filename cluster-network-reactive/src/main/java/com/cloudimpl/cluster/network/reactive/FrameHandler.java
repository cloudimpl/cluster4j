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

/**
 *
 * @author nuwansa
 */
public class FrameHandler {

    private ByteBuf cacheBuf = null;
    private static final int FRAME_LEN_SIZE = Integer.BYTES;

    public void onData(boolean client,ByteBuf byteBuf, FluxKeyProcessor sink) {
      //  System.out.println("ondata triggereed");
        if (this.cacheBuf != null) {
            this.cacheBuf.writeBytes(byteBuf);
        } else {
            this.cacheBuf = byteBuf;
        }
        this.cacheBuf.markReaderIndex();
        ByteBuf frame = readFrame(this.cacheBuf);
        while(frame != null)
        {
           // System.out.println("emit : ");
            long streamId = getStreamId(frame);
            boolean ok = sink.emit(client?streamId:-1,frame);
            if(!ok)
                System.out.println(" error submitting data: "+streamId);
            this.cacheBuf.discardReadBytes();
            this.cacheBuf.markReaderIndex();
            frame = readFrame(this.cacheBuf);
        }
        this.cacheBuf.resetReaderIndex();
        if(this.cacheBuf.readableBytes() > 0 && this.cacheBuf ==  byteBuf)
        {
            ByteBuf newBuf = ByteBufHandler.allocate();
            newBuf.writeBytes(this.cacheBuf,0,this.cacheBuf.readableBytes());
            this.cacheBuf = newBuf;
        }else if(this.cacheBuf.readableBytes() == 0 && this.cacheBuf != byteBuf)
        {
            this.cacheBuf.release();
            this.cacheBuf = null;
        }else
        {
            this.cacheBuf.clear();
        }
    }

    protected ByteBuf encodeFrame(long streamId,boolean request,int type,ByteBuf buf)
    {
        ByteBuf frame = ByteBufHandler.allocate();
        frame.writeInt(buf.readableBytes() +  Long.BYTES + Byte.BYTES + Byte.BYTES);
        frame.writeLong(streamId);
        frame.writeByte(type);
        frame.writeByte(request?1:2);
        frame.writeBytes(buf);
        buf.resetReaderIndex();
      //  System.out.println("encode frame :"+buf.memoryAddress());
        boolean ok = buf.release();
      //  System.out.println("encode frame :"+ok);
        return frame;
    }
    
    //protected ByteBuf encodeErrorFrame(long streamId,boolean )
    protected long getStreamId(ByteBuf frame)
    {
        frame.markReaderIndex();
        long len = frame.readLong();
        frame.resetReaderIndex();
        return len;
    }
    
    protected boolean isRequest(ByteBuf frame)
    {
        frame.markReaderIndex();
        frame.skipBytes(9);
        boolean req = frame.readByte() == 1;
        frame.resetReaderIndex();
        return req;
    }
    
    protected int isRequestType(ByteBuf frame)
    {
        frame.markReaderIndex();
        frame.skipBytes(8);
        int req = frame.readByte();
        frame.resetReaderIndex();
        return req;
    }
    
    private ByteBuf readFrame(ByteBuf buf) {
        if (buf.readableBytes() >= FRAME_LEN_SIZE) {
            int len = buf.readInt();
            if (len <= buf.readableBytes()) {
                ByteBuf frameBuf = ByteBufHandler.allocate();
                frameBuf.writeBytes(buf, len);
                return frameBuf;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }
}
