/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
import io.questdb.network.Net;
import io.questdb.std.Os;

/**
 *
 * @author nuwansa
 */
public class EpollTest implements XEventCallback{
    public static void main(String[] args) throws InterruptedException {
    //    System.setSecurityManager(new SecurityManagerImpl());

        Os.init();
        XEventLoop loop = XEventLoop.create(64);
        if(args[0].equals("s"))
        {
            System.out.println("create server");
           XUdpChannel channel =  XChannel.createUdpChannel(new EpollTest());
           channel.bind(12345);
           loop.add(channel);
        }
        else
        {
            System.out.println("create client");
            XUdpChannel channel = XChannel.createUdpChannel(null);
            long addr = Net.sockaddr("127.0.0.1", 12345);
            XByteBuffer buf = ByteBufferNative.allocate(1024);
            int i = 0;
            while(true)
            {
                buf.putInt(0,i);
                i++;
                channel.sendTo(addr, buf.addr(), 1024);
               // System.out.println("client sending: "+i);
               // Thread.sleep(1000);
            }
         //   loop.add(channel);
        }
        loop.run(false);
    }

    @Override
    public void onReadEvent(XEventLoop loop,XTcpClient channel, ByteBuf buf, int len) {
        System.out.println(channel+":on read event : "+len);
    }

    @Override
    public void onDisconnect(XEventLoop loop,XTcpClient channel) {
       System.out.println(channel+":on disconnected");
    }

    @Override
    public void onConnect(XEventLoop loop,XTcpClient channel) {
        System.out.println(channel+":onconnect");
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(10);
        buf.writeBytes("nuwan".getBytes(), 0, 5);
        channel.write(buf);
    }

    @Override
    public void onWriteReady(XEventLoop loop,XTcpClient channel) {
         System.out.println(channel+":write ready");
    }

    int rate = 0;
    long start = System.currentTimeMillis();
    @Override
    public void onReadEvent(XEventLoop loop,XUdpChannel channel, ByteBuf buf, int len) {
       // System.out.println(channel.id()+":udp on read event : "+len +" data: "+ buf.getInt(0));
       long end = System.currentTimeMillis();
       rate++;
       if(end - start >= 1000)
       {
           System.out.println("rate : "+rate);
           start = System.currentTimeMillis();
           rate = 0;
       }
    }
}
