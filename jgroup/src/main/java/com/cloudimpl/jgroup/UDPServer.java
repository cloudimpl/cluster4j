/*
 * Copyright 2021 nuwansa.
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
package com.cloudimpl.jgroup;

import com.cloudimpl.cluster4j.common.GsonCodec;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author nuwansa
 */
public class UDPServer {

    public static void main(String[] args) throws SocketException, IOException {
        Map<String, InetSocketAddress> meMap = new HashMap<>();
        DatagramSocket socket = new DatagramSocket(new InetSocketAddress("0.0.0.0", 12345));
        System.out.println("udp server started");
        while (true) {
            DatagramPacket pkt = new DatagramPacket(new byte[1024], 1024);
            socket.receive(pkt);
            System.out.println("packet receiving : " + pkt.getLength());
            if (pkt.getLength() > 0) {
                String s = new String(pkt.getData(), 0, pkt.getLength());
                Object obj = (ConnectMsg) GsonCodec.decode(s);
                if (obj instanceof ConnectMsg) {
                    ConnectMsg msg = (ConnectMsg) obj;
                    System.out.println("msg received : " + msg);
                    InetSocketAddress addr = (InetSocketAddress) pkt.getSocketAddress();
                    meMap.put(msg.getMe(), addr);
                    InetSocketAddress inet = meMap.get(msg.getConnect());
                    if (inet != null) {
                        ConnectAck ack = new ConnectAck(msg.getConnect(), inet.getHostString(), inet.getPort());
                        System.out.println("ack sent:" + ack);
                        byte[] reply = GsonCodec.encode(ack).getBytes();
                        DatagramPacket pkt2 = new DatagramPacket(reply, reply.length);
                        pkt2.setSocketAddress(pkt.getSocketAddress());
                        socket.send(pkt2);
                    }
                }
                else
                {
                    System.out.println("unknow msg: "+obj);
                }

            }
        }
    }
}
