/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

import io.netty.buffer.ByteBuf;

/**
 *
 * @author nuwansa
 */
public interface XEventCallback {
    default void onReadEvent(XEventLoop loop,XTcpClient channel,ByteBuf buf,int len){throw new UnsupportedOperationException("Not supported yet.");};
    default void onReadEvent(XEventLoop loop,XUdpChannel channel,ByteBuf buf,int len) {throw new UnsupportedOperationException("Not supported yet.");};
    default void onDisconnect(XEventLoop loop,XTcpClient channel){throw new UnsupportedOperationException("Not supported yet.");};
    default void onConnect(XEventLoop loop,XTcpClient channel){throw new UnsupportedOperationException("Not supported yet.");};
    default void onWriteReady(XEventLoop loop,XTcpClient channel){throw new UnsupportedOperationException("Not supported yet.");};
}
