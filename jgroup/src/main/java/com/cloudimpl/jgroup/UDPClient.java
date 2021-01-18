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
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nuwansa
 */
public class UDPClient {

    public static void main(String[] args) throws SocketException, IOException, InterruptedException {
        Map<String, InetSocketAddress> meMap = new HashMap<>();
        DatagramSocket socket = new DatagramSocket(new InetSocketAddress("0.0.0.0", 12347));
        int i = 0;
        ConnectMsg msg = new ConnectMsg("nuwan", "sanjeewa"+i++, true);
        byte[] r = GsonCodec.encodeWithType(msg).getBytes();
        DatagramPacket pkt = new DatagramPacket(r, r.length);
        pkt.setSocketAddress(new InetSocketAddress("192.168.8.103", 4321));
        
        while (true) {
            
            socket.send(pkt);
           // System.gc();
           // Thread.sleep(1000);
        }

    }

}
