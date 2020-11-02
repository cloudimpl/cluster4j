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
package com.cloudimpl.cluster.network;

import java.util.Queue;
import java.util.function.Consumer;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

/**
 *
 * @author nuwansa
 */
public class AsyncTask {
    private final Mono<AsyncTask> mono;
    private  MonoSink<AsyncTask> emitter = null;
    private final Consumer<AsyncTask> channelProvider;
    private Object attachment;
    private XEventLoop loop;
    private XChannel channel;
    public AsyncTask(Consumer<AsyncTask> channelProvider,Queue<AsyncTask> queue,XEventLoop loop)
    {
        this.channelProvider = channelProvider;
        this.loop = loop;
        mono = Mono.create(emit->{
            if(this.emitter != null)
                emit.error(new IllegalStateException("ChannelSink allows only a single Subscriber"));
            
            this.emitter = emit;
            if(queue != null)
                queue.add(this);
            
        });
    }
    
    public Mono<AsyncTask> asMono()
    {
        return this.mono;
    }
    
    public Consumer<AsyncTask> getChannelProvider()
    {
        return this.channelProvider;
    }
    
    public void success()
    {
        this.emitter.success(this);
    }
    
    public XChannel getChannel()
    {
        return this.channel;
    }

    public void setChannel(XChannel channel) {
        this.channel = channel;
    }

    public void setLoop(XEventLoop loop) {
        this.loop = loop;
    }

    public XEventLoop getLoop() {
        return loop;
    }
    
    public void error(Throwable thr)
    {
        this.emitter.error(thr);
    }

    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    public <T> T getAttachment() {
        return (T) attachment;
    }
    
    
}
