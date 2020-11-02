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
package com.cloudimpl.cluster.app;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 *
 * @author nuwansa
 */
public class BufferTest {

    public static void main(String[] args) {
//        XByteBuffer buf = ByteBufferNative.allocate(1024);
//        byte[] b = "nuwan".getBytes();
//        buf.put(b, 0, b.length);
    
Roaring64Bitmap rr = new Roaring64Bitmap();
rr.add(1,100);
        System.out.println(rr.rankLong(1) + " "+rr.select(98));
rr.add(1000);
rr.add(1234);
rr.add(1234);
rr.add(4234);
        System.out.println(Integer.MAX_VALUE);
        System.out.println(rr.select(0));
        System.out.println(rr.select(1));
        System.out.println(rr.select(2));
        rr.removeLong(1234);
        System.out.println(rr.select(1));
        
        
        
//        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
//        long start = System.currentTimeMillis();
//        int rate = 0;
//        while(true)
//        {
//            buf.retain();
//            Payload payload = ByteBufPayload.create(buf, null);
//            int ref = payload.refCnt();
////            if(ref != 2)
////            {
////                System.out.println("xxxx");
////            }
//            payload.release();
//            rate++;
//            long end = System.currentTimeMillis();
//            if( end - start >= 1000)
//            {
//                System.out.println("rate: "+rate);
//                rate = 0;
//                start = System.currentTimeMillis();
//            }
//        }

        System.setProperty("io.netty.leakDetection.level","DISABLED") ;
        PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        
       // ByteBuf buf2 = allocator.directBuffer(1024, 1024 * 1024);
       // System.out.println(buf2.memoryAddress());
//        System.out.println(allocator.isDirectBufferPooled());
     //   ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
     long start = System.currentTimeMillis();
     long rate = 0;
        while (true) {
            ByteBuf buf = allocator.directBuffer(1024, 1024);
            buf.release();
                        rate++;
            long end = System.currentTimeMillis();
            if (end - start >= 1000) {
                System.out.println("rate: " + rate);
                rate = 0;
                start = System.currentTimeMillis();
            }
        //    System.out.println("ref: "+buf.refCnt());
        }

        //System.out.println("ref: " + buf.refCnt());

    }
}
