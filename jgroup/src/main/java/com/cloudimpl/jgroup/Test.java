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

import org.jgroups.ChannelListener;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

/**
 *
 * @author nuwansa
 */
public class Test extends  ReceiverAdapter{
    @Override
    public void viewAccepted(View newView) {
        System.out.println("One more node joined. Current node size is: " + newView.getMembers().size());
        newView.getMembers().forEach(addr->System.out.println("addr:"+addr));
    }
    public static void main(String[] args) throws Exception {
        JChannel channel = new JChannel("src/main/resources/udp.xml");
        channel.setReceiver(new Test());
        channel.connect("Baeldung");
        channel.addChannelListener(new ChannelListener() {
            @Override
            public void channelConnected(JChannel jc) {
                System.out.println("channel connected : "+jc);
            }

            @Override
            public void channelDisconnected(JChannel jc) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void channelClosed(JChannel jc) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        });
    }
    
}
