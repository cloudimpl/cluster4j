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
package com.cloudimpl.cluster.common;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 *
 * @author nuwansa
 */
public class FluxProcessor<T> {
    private final List<FluxSink<T>> list;
    private final Flux<T> flux;
    public FluxProcessor() {
        list = new CopyOnWriteArrayList<>();
        flux = Flux.<T>create(emitter->{
            list.add(emitter);
        });
    }
    
    public void add(T t)
    {
        list.forEach(sink->sink.next(t));
    }
    
    public Flux<T> flux()
    {
        return flux;
    }
}
