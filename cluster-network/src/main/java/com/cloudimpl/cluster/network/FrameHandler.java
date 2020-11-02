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
package com.cloudimpl.cluster.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 *
 * @author nuwansa
 */
public  class FrameHandler {
    private final PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
    public ByteBuf encodeLen(ByteBuf buf)
    {
        ByteBuf writeBuf = allocator.directBuffer(4 * 1024, 1024 * 1024);
        writeBuf.writeInt(buf.readableBytes());
        writeBuf.writeBytes(buf);
        return writeBuf;
    }
    
    public void encodeLen(ByteBuf writeBuf,ByteBuf buf)
    {
        writeBuf.writeInt(buf.readableBytes());
        writeBuf.writeBytes(buf);
    }
    
    
    public int  decodeLen(ByteBuf buf)
    {
        return buf.getInt(0);
    }
}
