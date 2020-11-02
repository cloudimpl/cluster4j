/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.Os;
import java.io.Closeable;

/**
 *
 * @author nuwansa
 */
public class XChannel implements Closeable{

    public static final int SERVER_SOCKET = 1;
    public static final int TCP_CLIENT_SOCKET = 2;
    public static final int UDP_CHANNEL_SOCKET = 3;

    protected final long id;
    private final int type;
    protected XEventLoop loop;
    private final XEventCallback cb;
    private Object attachment = null;

    public XChannel(long id, int type, XEventCallback cb) {
        this.id = id;
        this.type = type;
        this.cb = cb;
    }

    protected long id() {
        return this.id;
    }

    public int type() {
        return this.type;
    }

    @Override
    public void close() {
        Net.close(id);
    }

    protected void setEventLoop(XEventLoop loop) {
        this.loop = loop;
    }

    protected XEventCallback getCb() {
        return cb;
    }

    public void setAttachment(Object attachment)
    {
        this.attachment = attachment;
    }
    
    public <T> T getAttachment()
    {
        return (T) this.attachment;
    }
    
    public static XChannel createTcpServer(int port, XEventCallback cb) {

        long fd = Net.socketTcp(false);
        boolean ok = Net.bindTcp(fd, 0, port);
        if (!ok) {
            throw NetworkError.instance(Os.errno(), "error binding tcp socket");
        }

        Net.configureNoLinger(fd);
        Net.setTcpNoDelay(fd, true);
        Net.listen(fd, 1024);
        return new XChannel(fd, SERVER_SOCKET, cb);
    }

    public static XUdpChannel createUdpChannel(XEventCallback cb) {
        long fd = Net.socketUdp();
        Net.configureNonBlocking(fd);
        Net.setReuseAddress(fd);
        Net.setReusePort(fd);
        Net.configureNonBlocking(fd);
        Net.setRcvBuf(fd, 2 * 1024 * 1024);
        Net.setSndBuf(fd, 2 * 1024 * 1024);

        return new XUdpChannel(fd, cb);
    }

    public static XUdpChannel createUdpChannel(int bufSize, XEventCallback cb) {
        long fd = Net.socketUdp();
        Net.configureNonBlocking(fd);
        Net.setReuseAddress(fd);
        Net.setReusePort(fd);
        Net.configureNonBlocking(fd);
        Net.setRcvBuf(fd, bufSize);
        Net.setSndBuf(fd, bufSize);

        return new XUdpChannel(fd, cb);
    }

    public static XTcpClient createTcpClient(String clientAddr, int port, XEventCallback cb) {
        long fd = Net.socketTcp(false);
        long sockAddress = NetworkFacadeImpl.INSTANCE.sockaddr(Net.parseIPv4(clientAddr), port);
        long ret = Net.connect(fd, sockAddress);
        System.out.println("connect : ret :" + ret + " error no : " + Os.errno());
        if (ret < 0 && Os.errno() != ErrorNo.EINPROGRESS) {
            throw NetworkError.instance(Os.errno(), "error connecting to :" + clientAddr + ":" + port);
        }
        return new XTcpClient(fd, cb);
    }

}
