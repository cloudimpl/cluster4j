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

import com.cloudimpl.cluster.network.ByteBufferNative;
import com.cloudimpl.cluster.network.XByteBuffer;
import com.cloudimpl.cluster.network.XChannel;
import com.cloudimpl.cluster.network.XEventCallback;
import com.cloudimpl.cluster.network.XEventLoop;
import com.cloudimpl.cluster.network.XTcpClient;
import com.cloudimpl.cluster.network.XUdpChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.questdb.std.Os;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author nuwansa
 */
public class TcpTest {
    public static void main(String[] args) {
        if(args.length > 0 && args[0].equals("s"))
            TcpTest.createServer();
        else
            TcpTest.createClient();
        
        
    }
    
    
    public static void createServer()
    {
        XEventLoop loop = XEventLoop.create(64);
        Os.setCurrentThreadAffinity(7);
        loop.add(XChannel.createTcpServer(1234,new XEventCallback() {
            @Override
            public void onReadEvent(XEventLoop loop, XTcpClient channel, ByteBuf buf, int len) {
            //    System.out.println("recv: "+buf.getInt(0));
                channel.write(buf);
            }

            @Override
            public void onReadEvent(XEventLoop loop, XUdpChannel channel, ByteBuf buf, int len) {
               
            }

            @Override
            public void onDisconnect(XEventLoop loop, XTcpClient channel) {
                System.out.println("channel disconnected "+channel);
            }

            @Override
            public void onConnect(XEventLoop loop, XTcpClient channel) {
                System.out.println("channel connected "+channel);
                
            }

            @Override
            public void onWriteReady(XEventLoop loop, XTcpClient channel) {
                
            }
        }));
        loop.run(false);
    }
    
    public static void createClient()
    {
         AtomicReference<XTcpClient> client = new AtomicReference();
        XEventLoop loop = XEventLoop.create(64);
        loop.add(XChannel.createTcpClient("127.0.0.1", 1234,new XEventCallback() {
            int i = 0;
            long start;
            int rate = 0;
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(1024,1024);
            @Override
            public void onReadEvent(XEventLoop loop, XTcpClient channel, ByteBuf buf, int len) {
                buf.clear();
                buf.writeInt(i++);
                rate++;
                long end = System.currentTimeMillis();
                if(end - start >= 1000)
                {
                    System.out.println("rate: "+rate);
                    rate = 0;
                    start = System.currentTimeMillis();
                }
       //         System.out.println("recv: "+buf.getInt(0));
                  channel.write(buf);
            }

            @Override
            public void onReadEvent(XEventLoop loop, XUdpChannel channel, ByteBuf buf, int len) {
               
            }

            @Override
            public void onDisconnect(XEventLoop loop, XTcpClient channel) {
                System.out.println("channel disconnected "+channel);
                client.set(null);
            }

            @Override
            public void onConnect(XEventLoop loop, XTcpClient channel) {
                System.out.println("channel connected "+channel);
                start = System.currentTimeMillis();
                
                client.set(channel);
                buf.writeInt(i++);
                channel.write(buf);
                buf.clear();
            }

            @Override
            public void onWriteReady(XEventLoop loop, XTcpClient channel) {
                
            }
        }));
        Os.setCurrentThreadAffinity(3);
        XByteBuffer buf = ByteBufferNative.allocate(1024);
        
//        loop.createTimer(1000, new XTimerCallback() {
//            int i = 0;
//            @Override
//            public void onTimer(XTimer timer) {
//                buf.putInt(0, i++);
//                if(client.get() != null)
//                {
//                    client.get().write(buf.addr(), 1024);
//                    
//                }
//            }
//        });
        loop.run(false);
    }
}
