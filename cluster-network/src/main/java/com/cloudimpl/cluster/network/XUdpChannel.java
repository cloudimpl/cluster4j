/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.std.Os;

/**
 *
 * @author nuwansa
 */
public class XUdpChannel extends XChannel{

    public XUdpChannel(long id,XEventCallback cb) {
        super(id, XChannel.UDP_CHANNEL_SOCKET,cb);
    }
    
    public void bind(int port)
    {
        boolean ok = Net.bindUdp(id, 0, port);
        if(!ok)
            throw NetworkError.instance(Os.errno(), "error binding udp socket on port: "+port);
    }
    
    public int sendTo(long addr,long ptr,int len)
    {
        return Net.sendTo(id, ptr, len, addr);
    }
}
