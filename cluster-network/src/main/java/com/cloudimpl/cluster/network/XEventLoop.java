/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

import io.questdb.std.Os;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public interface XEventLoop {
    
        public static XEventLoop create(int capacity)
        {
            if(Os.type == Os.FREEBSD  || Os.type == Os.OSX)
            {
                return new XKqueueEventLoop(capacity);
            }
            else{
                return new XEpollEventLoop(capacity);
            }
        }
        
        void add(XChannel channel);
        
        Mono<AsyncTask> pushAsynTask(Consumer<AsyncTask> channelProvider);
        
        void remove(XChannel channel);
        
        void watchForWriteEvent(XChannel channel);
        
        XTimer createTimer(long micro,XTimerCallback cb);
        
        void run(boolean polling);
        
}
