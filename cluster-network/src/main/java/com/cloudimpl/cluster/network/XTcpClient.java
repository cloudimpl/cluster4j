/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.questdb.network.Net;

/**
 *
 * @author nuwansa
 */
public class XTcpClient extends XChannel {

    private boolean connected;
    private ByteBuf writeBuffer;
    private XChannel serverSocket = null;

    public XTcpClient(long fd, XEventCallback cb) {
        super(fd, XChannel.TCP_CLIENT_SOCKET, cb);
        this.writeBuffer = null;
        this.connected = false;
    }

    public void setServerSocket(XChannel serverSocket) {
        this.serverSocket = serverSocket;
    }

    public XChannel getServerSocket() {
        return serverSocket;
    }

    public int write(ByteBuf buf) {
        ensureValid();
        if (writeBuffer != null) {
            writeBuffer.writeBytes(buf);
            return -2;
        }
        // System.out.println("socket fd write : "+buf.readableBytes());
        int ret = Net.send(id, buf.memoryAddress(), buf.readableBytes());
        if (ret < 0) {
            if (ret == Net.ERETRY) {
                ret = 0;
            } else {
                return -1;
            }
        }
        if (ret != buf.readableBytes()) {
            writeBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(4 * 1024, 1024 * 1024);
            writeBuffer.writeBytes(buf, ret, buf.readableBytes() - ret);
            loop.watchForWriteEvent(this);
        }
        return ret;
    }

    private void ensureValid() {
        if (loop == null) {
            throw new RuntimeException("event loop not attached");
        }
    }

    @Override
    public synchronized void close() {
        if (writeBuffer != null) {
            writeBuffer.release();
        }
        writeBuffer = null;
        super.close();
    }

    protected synchronized int flush() {
        ensureValid();
        if (writeBuffer != null) {
            int ret = Net.send(id, writeBuffer.memoryAddress(), writeBuffer.readableBytes());
            if (ret < 0) {
                if (ret == Net.ERETRY) {
                    ret = -2;
                } else {
                    return -1;
                }
            }
            if (ret == writeBuffer.readableBytes()) {
                writeBuffer.release();
                writeBuffer = null;
            } else if (ret > 0) {
                writeBuffer.readerIndex(ret);
                writeBuffer.discardReadBytes();
                return 1;
            }
        }
        return 0;
    }

    protected void setConnected(boolean connected) {
        this.connected = connected;
    }

    public boolean isConnected() {
        return connected;
    }

}
