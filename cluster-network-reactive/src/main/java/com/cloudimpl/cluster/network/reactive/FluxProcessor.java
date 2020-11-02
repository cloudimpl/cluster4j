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

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 *
 * @author nuwansa
 */
public class FluxProcessor<T> {
    private FluxSink<T> sink;
    private final Flux<T> flux;
    
    public FluxProcessor() {
        this.flux = Flux.create(emitter->{
            if(this.sink != null)
                emitter.error(new IllegalStateException("FluxProcessor allows only a single Subscriber"));
            this.sink = emitter;
        });
    }
    
    public boolean emit(T value)
    {
        this.sink.next(value);
        return true;
    }
    
    public void emit(Throwable thr)
    {
        this.sink.error(thr);
    }
    
    public Flux<T> asFlux()
    {
        return this.flux;
    }
}
