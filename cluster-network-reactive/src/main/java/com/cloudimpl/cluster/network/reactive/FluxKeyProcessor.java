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
package com.cloudimpl.cluster.network.reactive;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 *
 * @author nuwansa
 */
public class FluxKeyProcessor extends FluxProcessor<ByteBuf>{
    private final Map<Long,FluxSink<ByteBuf>> sinkMap; //Todo more efficient garbage less map
    private final Flux<ByteBuf> flux;
    
    public FluxKeyProcessor() {
        sinkMap = new ConcurrentHashMap<>();
        this.flux = Flux.create(emitter->{
            long val = emitter.currentContext().getOrDefault("key",-1L);
            if(val == -1)
                emitter.error(new IllegalStateException("FluxProcessor allows only with context that has attrbiute key"));
            sinkMap.put(val, emitter);
        //    System.out.println("subscrbie : "+val);
            Runnable runnable = emitter.currentContext().get("supplier");
            runnable.run();
            emitter.onDispose(()->sinkMap.remove(val));
        });
    }
    
    public boolean emit(long key,ByteBuf value)
    {
        if(key == -1)
            return super.emit(value);
        FluxSink<ByteBuf> sink = sinkMap.get(key);
        if(sink != null)
            sink.next(value);
        else
            return false;
        return true;
    }
    
    public void emit(Throwable thr)
    {
        sinkMap.values().stream().forEach(sink->sink.error(thr));      
    }
    
    public Flux<ByteBuf> asKeyFlux()
    {
        return this.flux;
    }
}
